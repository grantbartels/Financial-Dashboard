require('dotenv').config();
console.log('OpenAI key loaded:', !!process.env.OPENAI_API_KEY);
const express = require('express');
const axios = require('axios');
const OpenAI = require('openai');

const app = express();

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// ======================================================
// ENV / QUICKBOOKS SETUP
// ======================================================

const PORT = Number(process.env.PORT || 3000);
const CLIENT_ID = process.env.QB_CLIENT_ID || '';
const CLIENT_SECRET = process.env.QB_CLIENT_SECRET || '';
const REDIRECT_URI = process.env.QB_REDIRECT_URI || `http://localhost:${PORT}/callback`;
const QB_ENV = process.env.QB_ENV || 'sandbox';

const QB_BASE_URL =
  QB_ENV === 'production'
    ? 'https://quickbooks.api.intuit.com'
    : 'https://sandbox-quickbooks.api.intuit.com';

let accessToken = '';
let realmId = '';
let lastSyncTime = null;
let syncStatus = 'Not synced';

let billsCache = {
  data: null,
  fetchedAt: null,
};

const CACHE_TTL_MS = 60 * 1000; // 60 seconds

let dashboardHistory = [];
const HISTORY_LIMIT = 50;

if (!CLIENT_ID || !CLIENT_SECRET) {
  console.warn('Missing QB_CLIENT_ID or QB_CLIENT_SECRET in environment variables.');
}

// ======================================================
// CONFIG
// ======================================================

const CONFIG = {
  thresholds: {
    dueSoonDays: 3,
    dueWeekDays: 7,
    due14Days: 14,
    overdueHighDays: 14,
    overdueCriticalDays: 30,
    mediumBillAmount: 1000,
    largeBillAmount: 2500,
    concentrationMediumPct: 20,
    concentrationHighPct: 35,
    paymentHistoryLookbackLimit: 300,
    anomalyAmountVariancePct: 40,
    lowCashCoveragePct: 100,
    mediumCashCoveragePct: 150,
    healthyCurrentRatio: 1.5,
    weakCurrentRatio: 1.0,
  },
  weights: {
    overdueBase: 60,
    overduePerDay: 2,
    overdueMaxExtra: 20,
    dueToday: 55,
    dueSoon3: 35,
    dueSoon7: 20,
    dueSoon14: 10,
    mediumAmount: 10,
    largeAmount: 20,
    criticalVendor: 15,
    duplicateSuspected: 20,
    missingBillNo: 5,
    missingDueDate: 15,
    missingMemo: 1,
    missingTerms: 4,
    abnormalPaymentTiming: 12,
    lateVendorPattern: -8,
    healthyFinancials: -5,
    weakFinancials: 10,
    lowCashCoverage: 18,
    mediumCashCoverage: 8,
    abnormalAmount: 12,
  },
  actionCutoffs: {
    payNowMin: 75,
    paySoonMin: 40,
    reviewMin: 20,
    monitorMin: 10,
  },
  vendorCategories: {
    utilities: ['electric', 'power', 'water', 'utility', 'utilities', 'gas', 'internet', 'wifi', 'telecom'],
    rentLease: ['rent', 'lease', 'landlord', 'property'],
    insurance: ['insurance'],
    payrollRelated: ['payroll', 'adp', 'paychex', 'gusto', 'wages', 'salary'],
    inventoryMaterials: ['supply', 'supplies', 'inventory', 'materials', 'wholesale', 'lumber', 'parts'],
    maintenanceRepair: ['repair', 'repairs', 'maintenance', 'service', 'bodyshop', 'contractor'],
    discretionary: ['marketing', 'advertising', 'promo', 'software', 'subscription', 'saas'],
  },
  criticalVendorCategories: new Set([
    'Utilities',
    'Rent / Lease',
    'Insurance',
    'Payroll Related',
    'Inventory / Materials',
  ]),
  reasonPriority: {
    urgency: {
      'Cash cannot fully cover': 110,
      'Cash coverage is tight': 105,
      'Critical vendor category': 100,
      Overdue: 95,
      'Due today': 90,
      'Due in': 85,
      'Due within 7 days': 80,
      'Due within 14 days': 70,
      'Large bill': 65,
      'Medium bill': 55,
      'Abnormal payment timing': 60,
      'Weak financial health context': 58,
    },
    anomaly: {
      'Possible duplicate detected': 100,
      Duplicate: 95,
      'Same vendor + amount + bill date appears multiple times': 90,
      'Amount is unusually high vs vendor history': 85,
    },
    data: {
      'Missing due date': 90,
      'Missing invoice number': 50,
      'Missing terms': 40,
      'Missing memo': 10,
    },
  },
};

// ======================================================
// GENERIC HELPERS
// ======================================================

function getTodayOnly() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate());
}

function round2(num) {
  return Math.round((Number(num || 0) + Number.EPSILON) * 100) / 100;
}

function escapeHtml(value) {
  if (value === null || value === undefined) return '';
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatMoney(value, currency = 'USD') {
  const num = Number(value || 0);
  try {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency,
    }).format(num);
  } catch {
    return `$${num.toFixed(2)}`;
  }
}

function csvEscape(value) {
  if (value === null || value === undefined) return '""';
  const str = String(value).replace(/"/g, '""');
  return `"${str}"`;
}

function safeText(value, fallback = 'N/A') {
  if (value === null || value === undefined) return fallback;
  const str = String(value).trim();
  return str ? str : fallback;
}

function isMissingText(value) {
  return value === null || value === undefined || String(value).trim() === '' || String(value).trim() === 'N/A';
}

function formatBoolean(value) {
  return value ? 'Yes' : 'No';
}

function getDaysUntilDue(dueDate) {
  if (!dueDate) return null;
  const due = new Date(`${dueDate}T00:00:00`);
  if (Number.isNaN(due.getTime())) return null;
  const diffMs = due - getTodayOnly();
  return Math.ceil(diffMs / (1000 * 60 * 60 * 24));
}

function getAgingBucket(daysUntilDue) {
  if (daysUntilDue === null) return 'No Due Date';
  if (daysUntilDue < 0) return 'Overdue';
  if (daysUntilDue === 0) return 'Due Today';
  if (daysUntilDue <= 3) return 'Due in 1–3 Days';
  if (daysUntilDue <= 7) return 'Due in 4–7 Days';
  if (daysUntilDue <= 14) return 'Due in 8–14 Days';
  return 'Due Later';
}

function getAmountBucket(amount) {
  if (amount >= CONFIG.thresholds.largeBillAmount) return 'Large';
  if (amount >= CONFIG.thresholds.mediumBillAmount) return 'Medium';
  return 'Small';
}

function getPaymentStatus(balance, totalAmount) {
  if (balance <= 0) return 'Paid';
  if (balance < totalAmount) return 'Partially Paid';
  return 'Unpaid';
}

function normalizeText(value) {
  return String(value || '').toLowerCase().trim();
}

function containsAny(text, terms) {
  return terms.some((term) => text.includes(term));
}

function classifyVendorFromContext({ vendorName = '', account = '', categorySummary = '', memo = '' }) {
  const categories = CONFIG.vendorCategories;
  const vendorText = normalizeText(vendorName);
  const accountText = normalizeText(account);
  const categoryText = normalizeText(categorySummary);
  const memoText = normalizeText(memo);
  const financialText = `${accountText} ${categoryText}`.trim();
  const allText = `${financialText} ${vendorText} ${memoText}`.trim();

  if (containsAny(financialText, categories.utilities) || containsAny(allText, ['pg&e'])) return 'Utilities';
  if (containsAny(financialText, categories.rentLease)) return 'Rent / Lease';
  if (containsAny(financialText, categories.insurance) || containsAny(allText, categories.insurance)) return 'Insurance';
  if (containsAny(financialText, categories.payrollRelated) || containsAny(allText, categories.payrollRelated)) return 'Payroll Related';
  if (containsAny(financialText, categories.inventoryMaterials) || containsAny(allText, categories.inventoryMaterials)) return 'Inventory / Materials';
  if (containsAny(financialText, categories.maintenanceRepair) || containsAny(allText, categories.maintenanceRepair)) return 'Maintenance / Repair';
  if (containsAny(financialText, categories.discretionary) || containsAny(allText, categories.discretionary)) return 'Discretionary';
  return 'General';
}

function isCriticalVendorCategory(category) {
  return CONFIG.criticalVendorCategories.has(category);
}

function scoreReason(label, group) {
  const priorities = CONFIG.reasonPriority[group] || {};
  for (const prefix of Object.keys(priorities)) {
    if (label.startsWith(prefix)) return priorities[prefix];
  }
  return 0;
}

function buildDecisionReason(urgencyDrivers, anomalyFlags, dataQualityFlags) {
  const ranked = [];

  urgencyDrivers.forEach((label) => ranked.push({ label, score: scoreReason(label, 'urgency') }));
  anomalyFlags.forEach((label) => ranked.push({ label, score: scoreReason(label, 'anomaly') }));
  dataQualityFlags.forEach((label) => ranked.push({ label, score: scoreReason(label, 'data') }));

  const seen = new Set();
  const top = ranked
    .sort((a, b) => b.score - a.score)
    .filter((item) => {
      if (seen.has(item.label)) return false;
      seen.add(item.label);
      return true;
    })
    .slice(0, 2)
    .map((item) => item.label);

  return top.length ? top.join(' + ') : 'No immediate concern';
}

function average(values) {
  if (!values.length) return null;
  return round2(values.reduce((sum, value) => sum + Number(value || 0), 0) / values.length);
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return round2((sorted[mid - 1] + sorted[mid]) / 2);
  }
  return round2(sorted[mid]);
}

function safeDivide(a, b) {
  if (!b) return null;
  return round2(a / b);
}

function toDateOnly(dateLike) {
  if (!dateLike) return null;
  const d = new Date(dateLike);
  if (Number.isNaN(d.getTime())) return null;
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function daysBetween(start, end) {
  const s = toDateOnly(start);
  const e = toDateOnly(end);
  if (!s || !e) return null;
  const diffMs = e - s;
  return Math.round(diffMs / (1000 * 60 * 60 * 24));
}

// ======================================================
// QUICKBOOKS HELPERS
// ======================================================

async function qbGet(path, params = {}) {
  if (!accessToken || !realmId) {
    throw new Error('QuickBooks not connected');
  }

  return axios.get(`${QB_BASE_URL}${path}`, {
    params,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: 'application/json',
    },
  });
}

async function qbQuery(sql) {
  return qbGet(`/v3/company/${realmId}/query`, { query: sql });
}

function isBillsCacheFresh() {
  if (!billsCache.data || !billsCache.fetchedAt) return false;
  return Date.now() - billsCache.fetchedAt < CACHE_TTL_MS;
}

function clearBillsCache() {
  billsCache = {
    data: null,
    fetchedAt: null,
  };
}

async function getBillsFromQuickBooks(forceRefresh = false) {
  if (!forceRefresh && isBillsCacheFresh()) {
    return billsCache.data;
  }

  const response = await qbQuery('SELECT * FROM Bill MAXRESULTS 1000');
  const rawBills = response.data.QueryResponse?.Bill || [];
  const normalizedBills = normalizeQuickBooksBills(rawBills);

  billsCache = {
    data: normalizedBills,
    fetchedAt: Date.now(),
  };

  return normalizedBills;
}

async function getBillPaymentsFromQuickBooks() {
  try {
    const response = await qbQuery(`SELECT * FROM BillPayment MAXRESULTS ${CONFIG.thresholds.paymentHistoryLookbackLimit}`);
    return response.data.QueryResponse?.BillPayment || [];
  } catch {
    return [];
  }
}

async function getAccountsFromQuickBooks() {
  try {
    const response = await qbQuery('SELECT * FROM Account MAXRESULTS 1000');
    return response.data.QueryResponse?.Account || [];
  } catch {
    return [];
  }
}

async function getProfitAndLossReport() {
  try {
    const response = await qbGet(`/v3/company/${realmId}/reports/ProfitAndLoss`, {
      accounting_method: 'Accrual',
      summarize_column_by: 'Total',
    });
    return response.data || null;
  } catch {
    return null;
  }
}

async function getBalanceSheetReport() {
  try {
    const response = await qbGet(`/v3/company/${realmId}/reports/BalanceSheet`, {
      accounting_method: 'Accrual',
      summarize_column_by: 'Total',
    });
    return response.data || null;
  } catch {
    return null;
  }
}

// ======================================================
// STEP 5: PAYMENT BEHAVIOR LAYER
// ======================================================

function extractLinkedBillIdsFromPayment(payment) {
  const ids = new Set();
  const lines = Array.isArray(payment.Line) ? payment.Line : [];

  for (const line of lines) {
    const linked = Array.isArray(line.LinkedTxn) ? line.LinkedTxn : [];
    for (const txn of linked) {
      if (txn.TxnType === 'Bill' && txn.TxnId) ids.add(String(txn.TxnId));
    }
  }

  return [...ids];
}

function buildPaymentHistoryMetrics(rawBills, billPayments) {
  if (!Array.isArray(billPayments) || billPayments.length === 0) {
    return {
      available: false,
      reason: 'No BillPayment history was returned from QuickBooks.',
      metrics: null,
      vendorStats: {},
      billPaymentMap: {},
    };
  }

  const billById = new Map(rawBills.map((bill) => [String(bill.Id), bill]));
  const vendorStatsMap = new Map();
  const billPaymentMap = new Map();
  const allDaysToPay = [];
  let lateCount = 0;
  let analyzedPayments = 0;

  for (const payment of billPayments) {
    const paymentDate = payment.TxnDate || payment.MetaData?.CreateTime;
    const linkedBillIds = extractLinkedBillIdsFromPayment(payment);

    for (const billId of linkedBillIds) {
      const bill = billById.get(String(billId));
      if (!bill) continue;

      const vendorName = safeText(bill.VendorRef?.name, 'Unknown Vendor');
      const daysToPay = daysBetween(bill.TxnDate, paymentDate);
      const daysLate = bill.DueDate ? daysBetween(bill.DueDate, paymentDate) : null;
      const wasLate = daysLate !== null ? daysLate > 0 : false;
      if (daysToPay === null) continue;

      analyzedPayments += 1;
      allDaysToPay.push(daysToPay);
      if (wasLate) lateCount += 1;

      if (!vendorStatsMap.has(vendorName)) {
        vendorStatsMap.set(vendorName, {
          vendorName,
          paymentCount: 0,
          totalDaysToPay: 0,
          daysToPayValues: [],
          latePaymentCount: 0,
          avgDaysLateWhenLateValues: [],
        });
      }

      const vendorStat = vendorStatsMap.get(vendorName);
      vendorStat.paymentCount += 1;
      vendorStat.totalDaysToPay += daysToPay;
      vendorStat.daysToPayValues.push(daysToPay);
      if (wasLate) {
        vendorStat.latePaymentCount += 1;
        vendorStat.avgDaysLateWhenLateValues.push(daysLate);
      }

      billPaymentMap.set(String(billId), {
        paymentDate,
        daysToPay,
        daysLate,
        wasLate,
      });
    }
  }

  const vendorStats = {};
  for (const [vendorName, stat] of vendorStatsMap.entries()) {
    vendorStats[vendorName] = {
      vendorName,
      paymentCount: stat.paymentCount,
      averageDaysToPay: average(stat.daysToPayValues),
      medianDaysToPay: median(stat.daysToPayValues),
      latePaymentRate: stat.paymentCount ? round2((stat.latePaymentCount / stat.paymentCount) * 100) : 0,
      averageDaysLateWhenLate: average(stat.avgDaysLateWhenLateValues) || 0,
    };
  }

  return {
    available: analyzedPayments > 0,
    reason: analyzedPayments > 0 ? null : 'BillPayment records did not map cleanly to bills.',
    metrics: analyzedPayments > 0 ? {
      averageDaysToPay: average(allDaysToPay),
      latePaymentRate: round2((lateCount / analyzedPayments) * 100),
      analyzedPayments,
      vendorsTracked: Object.keys(vendorStats).length,
    } : null,
    vendorStats,
    billPaymentMap: Object.fromEntries(billPaymentMap.entries()),
  };
}

// ======================================================
// STEP 6: FINANCIAL HEALTH LAYER
// ======================================================

function flattenReportRows(rows = [], collector = []) {
  for (const row of rows) {
    if (row.Summary?.ColData?.length) {
      collector.push(row);
    }
    if (row.Rows?.Row?.length) {
      flattenReportRows(row.Rows.Row, collector);
    }
  }
  return collector;
}

function findReportValueByLabels(report, labelOptions) {
  if (!report?.Rows?.Row) return null;
  const rows = flattenReportRows(report.Rows.Row, []);

  for (const row of rows) {
    const label = normalizeText(row.Summary?.ColData?.[0]?.value || '');
    if (!label) continue;
    for (const option of labelOptions) {
      if (label.includes(normalizeText(option))) {
        const numericCell = row.Summary?.ColData?.find((cell, idx) => idx > 0 && cell?.value !== undefined);
        const value = Number(String(numericCell?.value || '').replace(/,/g, ''));
        if (Number.isFinite(value)) return value;
      }
    }
  }
  return null;
}

async function getStatementsService() {
  const [pnl, balanceSheet] = await Promise.all([
    getProfitAndLossReport(),
    getBalanceSheetReport(),
  ]);

  if (!pnl && !balanceSheet) {
    return {
      available: false,
      reason: 'P&L / Balance Sheet / Cash Flow integration could not be read from QuickBooks reports.',
      metrics: null,
    };
  }

  const currentAssets = findReportValueByLabels(balanceSheet, ['total current assets']);
  const currentLiabilities = findReportValueByLabels(balanceSheet, ['total current liabilities']);
  const totalAssets = findReportValueByLabels(balanceSheet, ['total assets']);
  const totalLiabilities = findReportValueByLabels(balanceSheet, ['total liabilities']);
  const netIncome = findReportValueByLabels(pnl, ['net income']);
  const income = findReportValueByLabels(pnl, ['total income', 'income']);
  const expenses = findReportValueByLabels(pnl, ['total expenses', 'expenses']);

  const workingCapital =
    currentAssets !== null && currentLiabilities !== null ? round2(currentAssets - currentLiabilities) : null;
  const currentRatio =
    currentAssets !== null && currentLiabilities !== null && currentLiabilities !== 0
      ? round2(currentAssets / currentLiabilities)
      : null;

  let financialHealthLabel = 'Unknown';
  if (currentRatio !== null) {
    if (currentRatio < CONFIG.thresholds.weakCurrentRatio) financialHealthLabel = 'Weak';
    else if (currentRatio >= CONFIG.thresholds.healthyCurrentRatio) financialHealthLabel = 'Healthy';
    else financialHealthLabel = 'Moderate';
  }

  return {
    available: true,
    reason: null,
    metrics: {
      currentAssets,
      currentLiabilities,
      workingCapital,
      currentRatio,
      totalAssets,
      totalLiabilities,
      netIncome,
      totalIncome: income,
      totalExpenses: expenses,
      financialHealthLabel,
    },
  };
}

// ======================================================
// STEP 7: LIQUIDITY / CASH LAYER
// ======================================================

async function getBankCashService() {
  const accounts = await getAccountsFromQuickBooks();
  if (!accounts.length) {
    return {
      available: false,
      reason: 'Bank-feed / cash integration could not read bank accounts from QuickBooks.',
      metrics: null,
    };
  }

  const bankAccounts = accounts.filter((account) => ['Bank', 'Other Current Asset'].includes(account.AccountType));
  const availableCash = round2(
    bankAccounts.reduce((sum, account) => sum + Number(account.CurrentBalance || account.CurrentBalanceWithSubAccounts || 0), 0)
  );

  return {
    available: bankAccounts.length > 0,
    reason: bankAccounts.length > 0 ? null : 'No bank / cash accounts were returned from QuickBooks.',
    metrics: {
      bankAccountCount: bankAccounts.length,
      availableCash,
      bankAccounts: bankAccounts.map((account) => ({
        name: account.Name,
        balance: round2(Number(account.CurrentBalance || account.CurrentBalanceWithSubAccounts || 0)),
      })),
    },
  };
}

function augmentBankCashMetrics(bankData, unpaidBills) {
  if (!bankData.available || !bankData.metrics) return bankData;

  const payNowBills = unpaidBills.filter((bill) => bill.recommendedAction === 'Pay Now');
  const paySoonBills = unpaidBills.filter((bill) => bill.recommendedAction === 'Pay Soon');
  const payNowAmount = round2(payNowBills.reduce((sum, bill) => sum + bill.balance, 0));
  const paySoonAmount = round2(paySoonBills.reduce((sum, bill) => sum + bill.balance, 0));
  const availableCash = Number(bankData.metrics.availableCash || 0);
  const cashCoveragePayNow = payNowAmount > 0 ? round2((availableCash / payNowAmount) * 100) : null;
  const cashCoveragePayNowSoon = (payNowAmount + paySoonAmount) > 0
    ? round2((availableCash / (payNowAmount + paySoonAmount)) * 100)
    : null;

  return {
    ...bankData,
    metrics: {
      ...bankData.metrics,
      payNowAmount,
      paySoonAmount,
      cashCoveragePayNow,
      cashCoveragePayNowSoon,
    },
  };
}

// ======================================================
// BILL EXTRACTION
// ======================================================

function getCategorySummary(lines = []) {
  if (!Array.isArray(lines) || lines.length === 0) return 'N/A';

  const categories = lines
    .map((line) => {
      if (line.AccountBasedExpenseLineDetail?.AccountRef?.name) {
        return line.AccountBasedExpenseLineDetail.AccountRef.name;
      }
      if (line.ItemBasedExpenseLineDetail?.ItemRef?.name) {
        return line.ItemBasedExpenseLineDetail.ItemRef.name;
      }
      if (line.Description) {
        return line.Description;
      }
      return null;
    })
    .filter(Boolean);

  return categories.length ? [...new Set(categories)].join(', ') : 'N/A';
}

function getPrimaryAccount(lines = []) {
  if (!Array.isArray(lines)) return 'N/A';
  for (const line of lines) {
    const name = line.AccountBasedExpenseLineDetail?.AccountRef?.name;
    if (name) return name;
  }
  return 'N/A';
}

function getLocationClass(bill) {
  return safeText(bill.DepartmentRef?.name || bill.ClassRef?.name);
}

function getMemo(bill) {
  return safeText(bill.PrivateNote || bill.Memo);
}

function getTerms(bill) {
  return safeText(bill.SalesTermRef?.name || bill.TermRef?.name);
}

// ✅ PASTE HERE
// ======================================================
// DATA NORMALIZATION LAYER
// ======================================================

function normalizeVendorName(name) {
  if (!name) return 'Unknown Vendor';

  return String(name)
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\bincorporated\b/gi, 'INC')
    .replace(/\binc\.\b/gi, 'INC')
    .replace(/\bllc\b/gi, 'LLC')
    .replace(/\bltd\.\b/gi, 'LTD');
}

function normalizeDateString(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function normalizeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeQuickBooksBill(bill) {
  return {
    ...bill,
    TxnDate: normalizeDateString(bill.TxnDate),
    DueDate: normalizeDateString(bill.DueDate),
    TotalAmt: normalizeNumber(bill.TotalAmt, 0),
    Balance: normalizeNumber(bill.Balance, 0),
    VendorRef: {
      ...bill.VendorRef,
      name: normalizeVendorName(bill.VendorRef?.name),
    },
  };
}

function normalizeQuickBooksBills(rawBills = []) {
  return rawBills.map(normalizeQuickBooksBill);
}

// ======================================================

function buildBaseProcessedBills(rawBills) {
  return rawBills.map((bill) => {
    const billId = safeText(bill.Id);
    const billNo = safeText(bill.DocNumber);
    const vendorId = safeText(bill.VendorRef?.value);
    const vendorName = safeText(bill.VendorRef?.name, 'Unknown Vendor');
    const billDate = safeText(bill.TxnDate);
    const dueDate = bill.DueDate || null;
    const createdAt = safeText(bill.MetaData?.CreateTime);
    const lastUpdated = safeText(bill.MetaData?.LastUpdatedTime);

    const originalAmount = Number(bill.TotalAmt || 0);
    const balance = Number(bill.Balance || 0);
    const amountPaid = Math.max(0, round2(originalAmount - balance));
    const currency = safeText(bill.CurrencyRef?.value, 'USD');
    const terms = getTerms(bill);
    const memo = getMemo(bill);
    const categorySummary = getCategorySummary(bill.Line);
    const account = getPrimaryAccount(bill.Line);
    const locationClass = getLocationClass(bill);
    const daysUntilDue = getDaysUntilDue(dueDate);
    const isOverdue = daysUntilDue !== null && daysUntilDue < 0;
    const agingBucket = getAgingBucket(daysUntilDue);
    const amountBucket = getAmountBucket(originalAmount);
    const vendorCategory = classifyVendorFromContext({
      vendorName,
      account,
      categorySummary,
      memo,
    });
    const isCriticalVendor = isCriticalVendorCategory(vendorCategory);

    return {
      billId,
      billNo,
      vendorId,
      vendorName,
      vendorCategory,
      isCriticalVendor,
      billDate,
      dueDate,
      createdAt,
      lastUpdated,
      originalAmount,
      balance,
      amountPaid,
      currency,
      paymentStatus: getPaymentStatus(balance, originalAmount),
      terms,
      memo,
      categorySummary,
      account,
      locationClass,
      daysUntilDue,
      isOverdue,
      agingBucket,
      amountBucket,
      dueIn3Days: daysUntilDue !== null && daysUntilDue >= 0 && daysUntilDue <= CONFIG.thresholds.dueSoonDays,
      dueIn7Days: daysUntilDue !== null && daysUntilDue >= 0 && daysUntilDue <= CONFIG.thresholds.dueWeekDays,
      dueIn14Days: daysUntilDue !== null && daysUntilDue >= 0 && daysUntilDue <= CONFIG.thresholds.due14Days,
    };
  });
}

// ======================================================
// DUPLICATE DETECTION
// ======================================================

function detectCurrentDuplicates(processedBills) {
  const duplicateMap = new Map();
  const byBillNo = new Map();
  const byVendorAmountDate = new Map();

  for (const bill of processedBills) {
    if (!isMissingText(bill.billNo)) {
      const billNoKey = `${bill.vendorName}__${bill.billNo}`;
      if (!byBillNo.has(billNoKey)) byBillNo.set(billNoKey, []);
      byBillNo.get(billNoKey).push(bill.billId);
    }

    const vendorAmountDateKey = `${bill.vendorName}__${round2(bill.originalAmount)}__${bill.billDate}`;
    if (!byVendorAmountDate.has(vendorAmountDateKey)) byVendorAmountDate.set(vendorAmountDateKey, []);
    byVendorAmountDate.get(vendorAmountDateKey).push(bill.billId);
  }

  for (const ids of byBillNo.values()) {
    if (ids.length > 1) {
      for (const id of ids) {
        if (!duplicateMap.has(id)) duplicateMap.set(id, []);
        duplicateMap.get(id).push('Duplicate bill number for same vendor');
      }
    }
  }

  for (const ids of byVendorAmountDate.values()) {
    if (ids.length > 1) {
      for (const id of ids) {
        if (!duplicateMap.has(id)) duplicateMap.set(id, []);
        duplicateMap.get(id).push('Same vendor + amount + bill date appears multiple times');
      }
    }
  }

  return duplicateMap;
}

// ======================================================
// STEP 8: ANOMALY / CONTROL LAYER
// ======================================================

function buildVendorAmountBenchmarks(processedBills) {
  const map = new Map();
  for (const bill of processedBills) {
    if (!map.has(bill.vendorName)) map.set(bill.vendorName, []);
    map.get(bill.vendorName).push(Number(bill.originalAmount || 0));
  }

  const benchmarks = {};
  for (const [vendorName, amounts] of map.entries()) {
    benchmarks[vendorName] = {
      averageAmount: average(amounts),
      medianAmount: median(amounts),
      sampleSize: amounts.length,
    };
  }
  return benchmarks;
}

// ======================================================
// RULE ENGINE
// ======================================================

function applyRulesToBill(bill, context) {
  const {
    duplicateWarnings = [],
    paymentHistory = null,
    statementData = null,
    bankData = null,
    vendorBenchmarks = {},
  } = context || {};

  const urgencyDrivers = [];
  const dataQualityFlags = [];
  const anomalyFlags = [];
  const ruleHits = [];
  let score = 0;

  const addScore = (points, label, group = 'urgency') => {
    score += points;
    ruleHits.push({ label, points, group });
    if (group === 'urgency') urgencyDrivers.push(label);
    if (group === 'data') dataQualityFlags.push(label);
    if (group === 'anomaly') anomalyFlags.push(label);
  };

  if (bill.isOverdue) {
    const extraDays = Math.min(
      Math.abs(bill.daysUntilDue) * CONFIG.weights.overduePerDay,
      CONFIG.weights.overdueMaxExtra
    );
    addScore(CONFIG.weights.overdueBase + extraDays, `Overdue by ${Math.abs(bill.daysUntilDue)} day(s)`);
  } else if (bill.daysUntilDue === 0) {
    addScore(CONFIG.weights.dueToday, 'Due today');
  } else if (bill.daysUntilDue !== null && bill.daysUntilDue <= CONFIG.thresholds.dueSoonDays) {
    addScore(CONFIG.weights.dueSoon3, `Due in ${bill.daysUntilDue} day(s)`);
  } else if (bill.daysUntilDue !== null && bill.daysUntilDue <= CONFIG.thresholds.dueWeekDays) {
    addScore(CONFIG.weights.dueSoon7, 'Due within 7 days');
  } else if (bill.daysUntilDue !== null && bill.daysUntilDue <= CONFIG.thresholds.due14Days) {
    addScore(CONFIG.weights.dueSoon14, 'Due within 14 days');
  }

  if (bill.originalAmount >= CONFIG.thresholds.largeBillAmount) {
    addScore(CONFIG.weights.largeAmount, 'Large bill');
  } else if (bill.originalAmount >= CONFIG.thresholds.mediumBillAmount) {
    addScore(CONFIG.weights.mediumAmount, 'Medium bill');
  }

  if (bill.isCriticalVendor) {
    addScore(CONFIG.weights.criticalVendor, `Critical vendor category: ${bill.vendorCategory}`);
  }

  if (isMissingText(bill.billNo)) {
    addScore(CONFIG.weights.missingBillNo, 'Missing invoice number', 'data');
  }

  if (!bill.dueDate) {
    addScore(CONFIG.weights.missingDueDate, 'Missing due date', 'data');
  }

  if (isMissingText(bill.memo)) {
    addScore(CONFIG.weights.missingMemo, 'Missing memo', 'data');
  }

  if (isMissingText(bill.terms)) {
    addScore(CONFIG.weights.missingTerms, 'Missing terms', 'data');
  }

  if (duplicateWarnings.length) {
    addScore(CONFIG.weights.duplicateSuspected, 'Possible duplicate detected', 'anomaly');
    duplicateWarnings.forEach((warning) => anomalyFlags.push(warning));
  }

  const vendorBehavior = paymentHistory?.vendorStats?.[bill.vendorName];
  if (vendorBehavior && bill.daysUntilDue !== null) {
    const avgDaysToPay = Number(vendorBehavior.averageDaysToPay || 0);
    if (bill.isOverdue && vendorBehavior.latePaymentRate >= 60) {
      addScore(CONFIG.weights.lateVendorPattern, `Vendor historically paid late (${vendorBehavior.latePaymentRate}%)`);
    }

    if (avgDaysToPay > 0 && bill.daysUntilDue >= 0 && avgDaysToPay > bill.daysUntilDue + 5) {
      addScore(CONFIG.weights.abnormalPaymentTiming, `Abnormal payment timing vs vendor history (${avgDaysToPay} avg days to pay)`);
    }
  }

  const financialHealth = statementData?.metrics?.financialHealthLabel;
  if (financialHealth === 'Weak') {
    addScore(CONFIG.weights.weakFinancials, 'Weak financial health context');
  } else if (financialHealth === 'Healthy') {
    addScore(CONFIG.weights.healthyFinancials, 'Healthy financial health context');
  }

  const availableCash = Number(bankData?.metrics?.availableCash || 0);
  if (bankData?.available) {
    if (availableCash > 0 && bill.balance > availableCash) {
      addScore(CONFIG.weights.lowCashCoverage, 'Cash cannot fully cover this bill');
    } else if (availableCash > 0) {
      const coveragePct = round2((availableCash / Math.max(bill.balance, 1)) * 100);
      if (coveragePct <= CONFIG.thresholds.mediumCashCoveragePct) {
        addScore(CONFIG.weights.mediumCashCoverage, 'Cash coverage is tight');
      }
    }
  }

  const benchmark = vendorBenchmarks[bill.vendorName];
  if (benchmark && benchmark.sampleSize >= 2 && benchmark.averageAmount) {
    const ratio = bill.originalAmount / benchmark.averageAmount;
    if (ratio >= 1 + CONFIG.thresholds.anomalyAmountVariancePct / 100) {
      addScore(CONFIG.weights.abnormalAmount, 'Amount is unusually high vs vendor history', 'anomaly');
    }
  }

  score = Math.min(Math.round(score), 100);

  const severeOverdue = bill.isOverdue && Math.abs(bill.daysUntilDue) >= CONFIG.thresholds.overdueHighDays;
  const criticalOverdue = bill.isOverdue && Math.abs(bill.daysUntilDue) >= CONFIG.thresholds.overdueCriticalDays;
  const hasReviewIssue = anomalyFlags.length > 0 || dataQualityFlags.includes('Missing due date');
  const hasNonBlockingDataIssue = dataQualityFlags.length > 0;

  let recommendedAction = 'Later';
  let decisionRank = 5;
  let riskLevel = 'Low';

  if (criticalOverdue || bill.daysUntilDue === 0 || score >= CONFIG.actionCutoffs.payNowMin) {
    recommendedAction = 'Pay Now';
    decisionRank = 1;
    riskLevel = 'High';
  } else if (score >= CONFIG.actionCutoffs.paySoonMin) {
    recommendedAction = 'Pay Soon';
    decisionRank = 2;
    riskLevel = bill.isCriticalVendor || severeOverdue ? 'High' : 'Medium';
  } else if (hasReviewIssue || score >= CONFIG.actionCutoffs.reviewMin) {
    recommendedAction = 'Review';
    decisionRank = 3;
    riskLevel = 'Medium';
  } else if (hasNonBlockingDataIssue || score >= CONFIG.actionCutoffs.monitorMin) {
    recommendedAction = 'Monitor';
    decisionRank = 4;
    riskLevel = 'Low';
  }

  const decisionReason = buildDecisionReason(urgencyDrivers, anomalyFlags, dataQualityFlags);

  const explanationParts = [];
  if (urgencyDrivers.length) explanationParts.push(`Urgency drivers: ${urgencyDrivers.join(', ')}`);
  if (anomalyFlags.length) explanationParts.push(`Anomalies: ${anomalyFlags.join(', ')}`);
  if (dataQualityFlags.length) explanationParts.push(`Data quality: ${dataQualityFlags.join(', ')}`);
  if (!explanationParts.length) explanationParts.push('No major urgency, anomaly, or data-quality concern detected');

  return {
    priorityScore: score,
    ruleHits,
    urgencyDrivers,
    anomalyFlags,
    dataQualityFlags,
    reviewFlags: [...anomalyFlags, ...dataQualityFlags],
    recommendedAction,
    decisionRank,
    riskLevel,
    decisionReason,
    explanationText: `${recommendedAction} because ${explanationParts.join(' | ')}.`,
    paymentBehaviorSnapshot: vendorBehavior || null,
  };
}

function buildDecisionBills(rawBills, services = {}) {
  const baseBills = buildBaseProcessedBills(rawBills);
  const duplicateMap = detectCurrentDuplicates(baseBills);
  const vendorBenchmarks = buildVendorAmountBenchmarks(baseBills);

  return baseBills.map((bill) => {
    const output = applyRulesToBill(bill, {
      duplicateWarnings: duplicateMap.get(bill.billId) || [],
      paymentHistory: services.paymentData,
      statementData: services.statementData,
      bankData: services.bankData,
      vendorBenchmarks,
    });
    return { ...bill, ...output };
  });
}

function sortBillsForDecision(bills) {
  return [...bills].sort((a, b) => {
    if (a.decisionRank !== b.decisionRank) return a.decisionRank - b.decisionRank;
    if (b.priorityScore !== a.priorityScore) return b.priorityScore - a.priorityScore;

    const aDays = a.daysUntilDue === null ? 99999 : a.daysUntilDue;
    const bDays = b.daysUntilDue === null ? 99999 : b.daysUntilDue;
    if (aDays !== bDays) return aDays - bDays;

    return b.balance - a.balance;
  });
}

// ======================================================
// KPI / ALERTS
// ======================================================

function buildKpiSummary(unpaidBills) {
  const totalUnpaid = round2(unpaidBills.reduce((sum, bill) => sum + bill.balance, 0));
  const overdueBills = unpaidBills.filter((bill) => bill.isOverdue);
  const overdueAmount = round2(overdueBills.reduce((sum, bill) => sum + bill.balance, 0));
  const overduePercentOfTotal = totalUnpaid > 0 ? round2((overdueAmount / totalUnpaid) * 100) : 0;

  const dueIn3DaysBills = unpaidBills.filter((bill) => bill.dueIn3Days);
  const dueIn7DaysBills = unpaidBills.filter((bill) => bill.dueIn7Days);
  const dueIn14DaysBills = unpaidBills.filter((bill) => bill.dueIn14Days);

  const overdueDays = overdueBills
    .map((bill) => Math.abs(Number(bill.daysUntilDue)))
    .filter((days) => Number.isFinite(days));

  const avgDaysOverdue = overdueDays.length
    ? round2(overdueDays.reduce((sum, days) => sum + days, 0) / overdueDays.length)
    : 0;

  const largestOverdueBill = overdueBills.length
    ? overdueBills.reduce((max, bill) => (bill.balance > max.balance ? bill : max), overdueBills[0])
    : null;

  const vendorTotals = new Map();
  for (const bill of unpaidBills) {
    vendorTotals.set(bill.vendorName, round2((vendorTotals.get(bill.vendorName) || 0) + bill.balance));
  }

  const vendorEntries = [...vendorTotals.entries()]
    .map(([vendorName, amount]) => ({
      vendorName,
      amount,
      percentOfTotal: totalUnpaid ? round2((amount / totalUnpaid) * 100) : 0,
    }))
    .sort((a, b) => b.amount - a.amount);

  const topVendor = vendorEntries[0] || null;
  let vendorConcentrationRisk = 'Low';
  if (topVendor) {
    if (topVendor.percentOfTotal >= CONFIG.thresholds.concentrationHighPct) vendorConcentrationRisk = 'High';
    else if (topVendor.percentOfTotal >= CONFIG.thresholds.concentrationMediumPct) vendorConcentrationRisk = 'Medium';
  }

  return {
    totalUnpaid,
    overdueBillsCount: overdueBills.length,
    overdueAmount,
    overduePercentOfTotal,
    dueIn3DaysCount: dueIn3DaysBills.length,
    dueIn3DaysAmount: round2(dueIn3DaysBills.reduce((sum, bill) => sum + bill.balance, 0)),
    dueIn7DaysCount: dueIn7DaysBills.length,
    dueIn7DaysAmount: round2(dueIn7DaysBills.reduce((sum, bill) => sum + bill.balance, 0)),
    dueIn14DaysCount: dueIn14DaysBills.length,
    dueIn14DaysAmount: round2(dueIn14DaysBills.reduce((sum, bill) => sum + bill.balance, 0)),
    averageDaysOverdue: avgDaysOverdue,
    largestOverdueBill,
    topVendor,
    vendorConcentrationRisk,
  };
}

function buildHistoricalSnapshot(unpaidBills, kpis, actionCounts) {
  return {
    capturedAt: new Date().toISOString(),
    unpaidBillsCount: unpaidBills.length,
    totalUnpaid: kpis.totalUnpaid,
    overdueBillsCount: kpis.overdueBillsCount,
    overdueAmount: kpis.overdueAmount,
    overduePercentOfTotal: kpis.overduePercentOfTotal,
    dueIn3DaysCount: kpis.dueIn3DaysCount,
    dueIn7DaysCount: kpis.dueIn7DaysCount,
    dueIn14DaysCount: kpis.dueIn14DaysCount,
    averageDaysOverdue: kpis.averageDaysOverdue,
    vendorConcentrationRisk: kpis.vendorConcentrationRisk,
    topVendorName: kpis.topVendor?.vendorName || null,
    topVendorAmount: kpis.topVendor?.amount || 0,
    payNowCount: actionCounts.payNow,
    paySoonCount: actionCounts.paySoon,
    reviewCount: actionCounts.review,
    monitorCount: actionCounts.monitor,
    laterCount: actionCounts.later,
  };
}

function recordDashboardSnapshot(snapshot) {
  dashboardHistory.unshift(snapshot);

  if (dashboardHistory.length > HISTORY_LIMIT) {
    dashboardHistory = dashboardHistory.slice(0, HISTORY_LIMIT);
  }
}

function getPreviousSnapshot() {
  return dashboardHistory.length > 1 ? dashboardHistory[1] : null;
}

function buildTrend(current, previous) {
  if (current == null || previous == null) return null;

  const delta = round2(current - previous);

  return {
    current,
    previous,
    delta,
    direction: delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat',
  };
}

function buildHistoricalComparisons(currentSnapshot, previousSnapshot) {
  if (!currentSnapshot || !previousSnapshot) return null;

  return {
    totalUnpaid: buildTrend(currentSnapshot.totalUnpaid, previousSnapshot.totalUnpaid),
    overdueAmount: buildTrend(currentSnapshot.overdueAmount, previousSnapshot.overdueAmount),
    overdueBillsCount: buildTrend(currentSnapshot.overdueBillsCount, previousSnapshot.overdueBillsCount),
    payNowCount: buildTrend(currentSnapshot.payNowCount, previousSnapshot.payNowCount),
    reviewCount: buildTrend(currentSnapshot.reviewCount, previousSnapshot.reviewCount),
    unpaidBillsCount: buildTrend(currentSnapshot.unpaidBillsCount, previousSnapshot.unpaidBillsCount),
  };
}

function renderTrendValue(trend, isMoney = false) {
  if (!trend) return 'No prior snapshot';

  const deltaText = isMoney
    ? formatMoney(trend.delta)
    : String(trend.delta);

  const direction =
    trend.direction === 'up' ? '↑ Up' :
    trend.direction === 'down' ? '↓ Down' :
    '→ Flat';

  return `${direction} (${deltaText})`;
}

function buildActionCounts(unpaidBills) {
  return {
    payNow: unpaidBills.filter((bill) => bill.recommendedAction === 'Pay Now').length,
    paySoon: unpaidBills.filter((bill) => bill.recommendedAction === 'Pay Soon').length,
    review: unpaidBills.filter((bill) => bill.recommendedAction === 'Review').length,
    monitor: unpaidBills.filter((bill) => bill.recommendedAction === 'Monitor').length,
    later: unpaidBills.filter((bill) => bill.recommendedAction === 'Later').length,
  };
}

function buildDataQualityCounts(unpaidBills) {
  return {
    missingInvoiceNumber: unpaidBills.filter((b) => b.dataQualityFlags.includes('Missing invoice number')).length,
    missingDueDate: unpaidBills.filter((b) => b.dataQualityFlags.includes('Missing due date')).length,
    missingTerms: unpaidBills.filter((b) => b.dataQualityFlags.includes('Missing terms')).length,
    missingMemo: unpaidBills.filter((b) => b.dataQualityFlags.includes('Missing memo')).length,
  };
}

function buildAlerts(unpaidBills, kpis, paymentData, statementData, bankData, dataQualityCounts) {
  const alerts = [];

  if (kpis.overduePercentOfTotal >= 40) {
    alerts.push({
      severity: 'High',
      title: 'High overdue exposure',
      detail: `${kpis.overduePercentOfTotal}% of unpaid bills are overdue by dollar value.`,
    });
  }

  if (kpis.vendorConcentrationRisk === 'High' && kpis.topVendor) {
    alerts.push({
      severity: 'High',
      title: 'High vendor concentration',
      detail: `${kpis.topVendor.vendorName} represents ${kpis.topVendor.percentOfTotal}% of unpaid exposure.`,
    });
  }

  const duplicateCount = unpaidBills.filter((b) => b.anomalyFlags.some((flag) => flag.toLowerCase().includes('duplicate'))).length;
  if (duplicateCount > 0) {
    alerts.push({
      severity: 'Medium',
      title: 'Possible duplicates detected',
      detail: `${duplicateCount} unpaid bill(s) have duplicate warnings.`,
    });
  }

  const abnormalAmountCount = unpaidBills.filter((b) => b.anomalyFlags.includes('Amount is unusually high vs vendor history')).length;
  if (abnormalAmountCount > 0) {
    alerts.push({
      severity: 'Medium',
      title: 'Abnormal bill amounts detected',
      detail: `${abnormalAmountCount} unpaid bill(s) appear unusually high versus vendor history.`,
    });
  }

  const dataQualityDetailParts = [];
  if (dataQualityCounts.missingInvoiceNumber) dataQualityDetailParts.push(`${dataQualityCounts.missingInvoiceNumber} missing invoice number`);
  if (dataQualityCounts.missingDueDate) dataQualityDetailParts.push(`${dataQualityCounts.missingDueDate} missing due date`);
  if (dataQualityCounts.missingTerms) dataQualityDetailParts.push(`${dataQualityCounts.missingTerms} missing terms`);
  if (dataQualityCounts.missingMemo) dataQualityDetailParts.push(`${dataQualityCounts.missingMemo} missing memo`);

  if (dataQualityDetailParts.length) {
    alerts.push({
      severity: 'Medium',
      title: 'Bills need data cleanup',
      detail: dataQualityDetailParts.join(' • '),
    });
  }

  if (paymentData.available && paymentData.metrics) {
    alerts.push({
      severity: 'Info',
      title: 'Payment behavior layer active',
      detail: `Analyzed ${paymentData.metrics.analyzedPayments} bill payments across ${paymentData.metrics.vendorsTracked} vendors.`,
    });
  } else {
    alerts.push({ severity: 'Info', title: 'Payment history not connected', detail: paymentData.reason });
  }

  if (statementData.available && statementData.metrics) {
    alerts.push({
      severity: statementData.metrics.financialHealthLabel === 'Weak' ? 'Medium' : 'Info',
      title: 'Financial health layer active',
      detail: `Current ratio: ${statementData.metrics.currentRatio ?? 'N/A'} | Health: ${statementData.metrics.financialHealthLabel}`,
    });
  } else {
    alerts.push({ severity: 'Info', title: 'Financial statements not connected', detail: statementData.reason });
  }

  if (bankData.available && bankData.metrics) {
    const coverage = bankData.metrics.cashCoveragePayNow;
    let severity = 'Info';
    if (coverage !== null && coverage < CONFIG.thresholds.lowCashCoveragePct) severity = 'High';
    else if (coverage !== null && coverage < CONFIG.thresholds.mediumCashCoveragePct) severity = 'Medium';

    alerts.push({
      severity,
      title: 'Liquidity layer active',
      detail: `Available cash: ${formatMoney(bankData.metrics.availableCash)} | Pay Now coverage: ${coverage ?? 'N/A'}%`,
    });
  } else {
    alerts.push({ severity: 'Info', title: 'Bank / cash data not connected', detail: bankData.reason });
  }

  return alerts;
}

// ======================================================
// RENDER HELPERS
// ======================================================

function renderList(items, fallback = 'None') {
  if (!Array.isArray(items) || !items.length) return fallback;
  return items.map((item) => escapeHtml(item)).join('<br>');
}

function renderRuleHits(ruleHits) {
  if (!Array.isArray(ruleHits) || !ruleHits.length) return 'None';
  return ruleHits
    .map((hit) => `${escapeHtml(hit.label)} (+${escapeHtml(String(hit.points))}) [${escapeHtml(hit.group)}]`)
    .join('<br>');
}

function renderUnavailableCard(title, reason) {
  return `
    <div class="kpi-card unavailable">
      <strong>${escapeHtml(title)}</strong><br>
      <span>${escapeHtml(reason)}</span>
    </div>
  `;
}

function renderAlerts(alerts) {
  if (!alerts.length) return '<p>No alerts.</p>';
  return alerts.map((alert) => {
    const cls = alert.severity === 'High'
      ? 'alert-high'
      : alert.severity === 'Medium'
        ? 'alert-medium'
        : 'alert-info';

    return `
      <div class="alert-card ${cls}">
        <strong>${escapeHtml(alert.severity)} — ${escapeHtml(alert.title)}</strong><br>
        <span>${escapeHtml(alert.detail)}</span>
      </div>
    `;
  }).join('');
}

function buildDashboardData(rawBills, services) {
  const decisionBills = buildDecisionBills(rawBills, services);
  const unpaidBills = sortBillsForDecision(decisionBills.filter((bill) => bill.balance > 0));
  const kpis = buildKpiSummary(unpaidBills);
  const actionCounts = buildActionCounts(unpaidBills);
  const dataQualityCounts = buildDataQualityCounts(unpaidBills);
  return { unpaidBills, kpis, actionCounts, dataQualityCounts };
}

async function buildAllServices(rawBills) {
  const [billPayments, statementDataRaw, bankDataRaw] = await Promise.all([
    getBillPaymentsFromQuickBooks(),
    getStatementsService(),
    getBankCashService(),
  ]);

  const paymentData = buildPaymentHistoryMetrics(rawBills, billPayments);
  const initialServices = {
    paymentData,
    statementData: statementDataRaw,
    bankData: bankDataRaw,
  };

  const firstPass = buildDashboardData(rawBills, initialServices);
  const bankData = augmentBankCashMetrics(bankDataRaw, firstPass.unpaidBills);

  return {
    paymentData,
    statementData: statementDataRaw,
    bankData,
  };
}

// ======================================================
// AI SUMMARY LAYER
// ======================================================

async function generateAiSummary({ unpaidBills, kpis, actionCounts, alerts, services }) {
  try {
    if (!process.env.OPENAI_API_KEY) {
      return 'AI summary unavailable: OPENAI_API_KEY missing.';
    }

    const topBills = unpaidBills.slice(0, 5).map((bill) => ({
      vendor: bill.vendorName,
      category: bill.vendorCategory,
      balance: bill.balance,
      action: bill.recommendedAction,
      risk: bill.riskLevel,
      score: bill.priorityScore,
      reason: bill.decisionReason,
      dueDate: bill.dueDate,
      daysUntilDue: bill.daysUntilDue,
      overdue: bill.isOverdue,
    }));

    const payload = {
      summary: {
        unpaidBills: unpaidBills.length,
        totalUnpaid: kpis.totalUnpaid,
        overdueBills: kpis.overdueBillsCount,
        overdueAmount: kpis.overdueAmount,
        overduePercent: kpis.overduePercentOfTotal,
        due3: kpis.dueIn3DaysCount,
        due7: kpis.dueIn7DaysCount,
        due14: kpis.dueIn14DaysCount,
        vendorRisk: kpis.vendorConcentrationRisk,
        topVendor: kpis.topVendor,
      },
      actions: actionCounts,
      alerts,
      topBills,
      financialHealth: services.statementData?.metrics || null,
      liquidity: services.bankData?.metrics || null,
      paymentBehavior: services.paymentData?.metrics || null,
    };

    const prompt = `
You are a finance operations assistant.

Analyze this accounts payable data and output:

1. Executive Summary
2. Top 3 Risks
3. Top 3 Actions
4. Cash Flow Warning (if needed)

Rules:
- Be concise
- Be practical
- No fluff
- Only use given data

Data:
${JSON.stringify(payload, null, 2)}
`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4.1-mini',
      temperature: 0.2,
      messages: [
        { role: 'system', content: 'You are a precise AP finance assistant.' },
        { role: 'user', content: prompt },
      ],
    });

    return response.choices?.[0]?.message?.content || 'AI summary unavailable.';
  } catch (err) {
    return `AI error: ${err.response?.data ? JSON.stringify(err.response.data) : err.message}`;
  }
}

// ======================================================
// STEP 9: PRODUCTIZATION HELPERS
// ======================================================

function getSystemStageLabel() {
  return 'Step 9 Finance Blueprint';
}

// ======================================================
// OAUTH ROUTES
// ======================================================

app.get('/', (req, res) => {
  const url =
    `https://appcenter.intuit.com/connect/oauth2` +
    `?client_id=${encodeURIComponent(CLIENT_ID || '')}` +
    `&response_type=code` +
    `&scope=${encodeURIComponent('com.intuit.quickbooks.accounting')}` +
    `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
    `&state=finance-blueprint-step9`;

  res.send(`
    <html>
      <head>
        <title>${getSystemStageLabel()}</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 30px; }
          a.button {
            display: inline-block;
            padding: 12px 18px;
            background: #2b6cb0;
            color: white;
            text-decoration: none;
            border-radius: 6px;
          }
        </style>
      </head>
      <body>
        <h1>${getSystemStageLabel()}</h1>
        <p>Connect QuickBooks, then open the dashboard.</p>
        <a class="button" href="${url}">Connect QuickBooks</a>
      </body>
    </html>
  `);
});

app.get('/callback', async (req, res) => {
  try {
    const code = req.query.code;
    realmId = req.query.realmId;

    if (!code || !realmId) {
      return res.status(400).send('Missing code or realmId from QuickBooks callback.');
    }

    const tokenRes = await axios.post(
      'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
      `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`,
      {
        headers: {
          Authorization: 'Basic ' + Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded',
          Accept: 'application/json',
        },
      }
    );

    accessToken = tokenRes.data.access_token;

    res.send(`
      <html>
        <head>
          <title>Connected</title>
          <style>
            body { font-family: Arial, sans-serif; padding: 30px; }
            a.button {
              display: inline-block;
              padding: 12px 18px;
              background: #2f855a;
              color: white;
              text-decoration: none;
              border-radius: 6px;
            }
          </style>
        </head>
        <body>
          <h1>Connected to QuickBooks</h1>
          <p><strong>Realm ID:</strong> ${escapeHtml(realmId)}</p>
          <a class="button" href="/bills">Open Dashboard</a>
        </body>
      </html>
    `);
  } catch (err) {
    res.status(500).send(`<pre>${escapeHtml(err.response?.data ? JSON.stringify(err.response.data, null, 2) : err.message)}</pre>`);
  }
});

// ======================================================
// DASHBOARD
// ======================================================

app.get('/bills', async (req, res) => {
  try {
    if (!accessToken || !realmId) {
      return res.send(`
        <html>
          <body style="font-family: Arial, sans-serif; padding: 30px;">
            <h1>Not connected</h1>
            <p>Please connect QuickBooks first.</p>
            <a href="/">Go Home</a>
          </body>
        </html>
      `);
    }

    const forceRefresh = req.query.refresh === 'true';
const rawBills = await getBillsFromQuickBooks(forceRefresh);
    lastSyncTime = new Date();
    syncStatus = 'Success';

    const services = await buildAllServices(rawBills);
    const { unpaidBills, kpis, actionCounts, dataQualityCounts } = buildDashboardData(rawBills, services);
    const alerts = buildAlerts(
      unpaidBills,
      kpis,
      services.paymentData,
      services.statementData,
      services.bankData,
      dataQualityCounts
    );

    const currentSnapshot = buildHistoricalSnapshot(unpaidBills, kpis, actionCounts);
recordDashboardSnapshot(currentSnapshot);

const previousSnapshot = getPreviousSnapshot();
const historicalComparisons =
  buildHistoricalComparisons(currentSnapshot, previousSnapshot);

    const runAI = req.query.ai === 'true';

let aiSummary = 'AI summary not generated yet. Click "Generate AI Summary" to run it.';

if (runAI) {
  aiSummary = await generateAiSummary({
    unpaidBills,
    kpis,
    actionCounts,
    alerts,
    services,
  });
}

    const rows = unpaidBills.map((bill) => {
      let rowClass = '';
      if (bill.recommendedAction === 'Pay Now') rowClass = 'pay-now';
      else if (bill.recommendedAction === 'Pay Soon') rowClass = 'pay-soon';
      else if (bill.recommendedAction === 'Review') rowClass = 'review';
      else if (bill.recommendedAction === 'Monitor') rowClass = 'monitor';

      return `
        <tr class="${rowClass}">
          <td>${escapeHtml(String(bill.decisionRank))}</td>
          <td>${escapeHtml(bill.recommendedAction)}</td>
          <td>${escapeHtml(bill.riskLevel)}</td>
          <td>${escapeHtml(String(bill.priorityScore))}</td>
          <td>${escapeHtml(bill.vendorCategory)}</td>
          <td>${escapeHtml(formatBoolean(bill.isCriticalVendor))}</td>
          <td>${escapeHtml(bill.vendorName)}</td>
          <td>${escapeHtml(bill.billNo)}</td>
          <td>${escapeHtml(bill.billDate)}</td>
          <td>${escapeHtml(bill.dueDate || 'No Due Date')}</td>
          <td>${escapeHtml(String(bill.daysUntilDue ?? 'N/A'))}</td>
          <td>${escapeHtml(formatBoolean(bill.isOverdue))}</td>
          <td>${escapeHtml(bill.agingBucket)}</td>
          <td>${escapeHtml(bill.amountBucket)}</td>
          <td>${formatMoney(bill.originalAmount, bill.currency)}</td>
          <td>${formatMoney(bill.amountPaid, bill.currency)}</td>
          <td>${formatMoney(bill.balance, bill.currency)}</td>
          <td>${escapeHtml(bill.paymentStatus)}</td>
          <td>${escapeHtml(bill.terms)}</td>
          <td>${escapeHtml(bill.categorySummary)}</td>
          <td>${escapeHtml(bill.account)}</td>
          <td>${escapeHtml(bill.locationClass)}</td>
          <td>${escapeHtml(bill.memo)}</td>
          <td>${renderList(bill.urgencyDrivers)}</td>
          <td>${renderList(bill.anomalyFlags)}</td>
          <td>${renderList(bill.dataQualityFlags)}</td>
          <td>${renderRuleHits(bill.ruleHits)}</td>
          <td>${escapeHtml(bill.decisionReason)}</td>
          <td>${escapeHtml(bill.explanationText)}</td>
        </tr>
      `;
    }).join('');

    const paymentMetrics = services.paymentData.metrics;
    const statementMetrics = services.statementData.metrics;
    const bankMetrics = services.bankData.metrics;

    res.send(`
      <html>
        <head>
          <title>${getSystemStageLabel()} Dashboard</title>
          <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            h1, h2 { margin-bottom: 10px; }
            .summary p { margin: 6px 0; }
            .decision-summary, .kpi-grid, .section-grid, .quality-grid {
              display: flex;
              gap: 12px;
              flex-wrap: wrap;
              margin: 15px 0 20px 0;
            }
            .decision-card, .kpi-card {
              border: 1px solid #ccc;
              border-radius: 8px;
              padding: 12px 14px;
              min-width: 180px;
              background: #fafafa;
            }
            .unavailable { background: #f8f8f8; color: #555; }
            .panel {
              margin: 18px 0;
              padding: 14px;
              border: 1px solid #ddd;
              border-radius: 8px;
              background: #fcfcfc;
            }
            .alert-card {
              border-radius: 8px;
              padding: 10px 12px;
              border: 1px solid #ccc;
              margin-bottom: 10px;
            }
            .alert-high { background: #ffe9e9; border-color: #e2a0a0; }
            .alert-medium { background: #fff7e6; border-color: #e6c982; }
            .alert-info { background: #eef6ff; border-color: #9ec2ea; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 14px; }
            th, td {
              border: 1px solid #ccc;
              padding: 8px;
              text-align: left;
              vertical-align: top;
              white-space: nowrap;
            }
            th { background: #f5f5f5; position: sticky; top: 0; }
            .table-wrap { overflow-x: auto; max-width: 100%; }
            .pay-now { background: #ffe5e5; }
            .pay-soon { background: #fff4db; }
            .review { background: #fffbe6; }
            .monitor { background: #eef7ff; }
            a.button {
              display: inline-block;
              margin-top: 15px;
              margin-right: 10px;
              padding: 10px 16px;
              background: #2b6cb0;
              color: white;
              text-decoration: none;
              border-radius: 6px;
            }
          </style>
        </head>
        <body>
          <h1>${getSystemStageLabel()} Dashboard</h1>

          <div class="summary">
            <p><strong>Unpaid Bills:</strong> ${unpaidBills.length}</p>
            <p><strong>Total Unpaid:</strong> ${formatMoney(kpis.totalUnpaid)}</p>
            <p><strong>Last Sync Time:</strong> ${lastSyncTime ? escapeHtml(lastSyncTime.toLocaleString()) : 'N/A'}</p>
            <p><strong>Sync Status:</strong> ${escapeHtml(syncStatus)}</p>

          </div>

          <div class="decision-summary">
            <div class="decision-card"><strong>Pay Now</strong><br>${actionCounts.payNow}</div>
            <div class="decision-card"><strong>Pay Soon</strong><br>${actionCounts.paySoon}</div>
            <div class="decision-card"><strong>Review</strong><br>${actionCounts.review}</div>
            <div class="decision-card"><strong>Monitor</strong><br>${actionCounts.monitor}</div>
            <div class="decision-card"><strong>Later</strong><br>${actionCounts.later}</div>
          </div>

          <div class="panel">
            <h2>AP KPI Summary</h2>
            <div class="kpi-grid">
              <div class="kpi-card"><strong>Overdue Bills</strong><br>${kpis.overdueBillsCount}</div>
              <div class="kpi-card"><strong>Overdue Amount</strong><br>${formatMoney(kpis.overdueAmount)}</div>
              <div class="kpi-card"><strong>Overdue % of Total</strong><br>${kpis.overduePercentOfTotal}%</div>
              <div class="kpi-card"><strong>Due Next 3 Days</strong><br>${kpis.dueIn3DaysCount} / ${formatMoney(kpis.dueIn3DaysAmount)}</div>
              <div class="kpi-card"><strong>Due Next 7 Days</strong><br>${kpis.dueIn7DaysCount} / ${formatMoney(kpis.dueIn7DaysAmount)}</div>
              <div class="kpi-card"><strong>Due Next 14 Days</strong><br>${kpis.dueIn14DaysCount} / ${formatMoney(kpis.dueIn14DaysAmount)}</div>
              <div class="kpi-card"><strong>Avg Days Overdue</strong><br>${kpis.averageDaysOverdue}</div>
              <div class="kpi-card"><strong>Vendor Concentration Risk</strong><br>${escapeHtml(kpis.vendorConcentrationRisk)}</div>
              <div class="kpi-card"><strong>Top Vendor Exposure</strong><br>${
                kpis.topVendor
                  ? `${escapeHtml(kpis.topVendor.vendorName)} — ${formatMoney(kpis.topVendor.amount)} (${kpis.topVendor.percentOfTotal}%)`
                  : 'N/A'
              }</div>
              <div class="kpi-card"><strong>Largest Overdue Bill</strong><br>${
                kpis.largestOverdueBill
                  ? `${escapeHtml(kpis.largestOverdueBill.vendorName)} — ${formatMoney(kpis.largestOverdueBill.balance, kpis.largestOverdueBill.currency)}`
                  : 'N/A'
              }</div>
            </div>
          </div>

<div class="panel">
  <h2>Historical Trend Snapshot</h2>
  ${
    historicalComparisons
      ? `
        <div class="kpi-grid">
          <div class="kpi-card">
            <strong>Total Unpaid</strong><br>
            ${escapeHtml(renderTrendValue(historicalComparisons.totalUnpaid, true))}
          </div>
          <div class="kpi-card">
            <strong>Overdue Amount</strong><br>
            ${escapeHtml(renderTrendValue(historicalComparisons.overdueAmount, true))}
          </div>
          <div class="kpi-card">
            <strong>Overdue Bills</strong><br>
            ${escapeHtml(renderTrendValue(historicalComparisons.overdueBillsCount))}
          </div>
          <div class="kpi-card">
            <strong>Pay Now</strong><br>
            ${escapeHtml(renderTrendValue(historicalComparisons.payNowCount))}
          </div>
          <div class="kpi-card">
            <strong>Review</strong><br>
            ${escapeHtml(renderTrendValue(historicalComparisons.reviewCount))}
          </div>
          <div class="kpi-card">
            <strong>Total Bills</strong><br>
            ${escapeHtml(renderTrendValue(historicalComparisons.unpaidBillsCount))}
          </div>
        </div>
      `
      : `<p>No previous snapshot yet. Refresh again to see trends.</p>`
  }
</div>

          <div class="panel">
            <h2>Data Quality Summary</h2>
            <div class="quality-grid">
              <div class="kpi-card"><strong>Missing Invoice Number</strong><br>${dataQualityCounts.missingInvoiceNumber}</div>
              <div class="kpi-card"><strong>Missing Due Date</strong><br>${dataQualityCounts.missingDueDate}</div>
              <div class="kpi-card"><strong>Missing Terms</strong><br>${dataQualityCounts.missingTerms}</div>
              <div class="kpi-card"><strong>Missing Memo</strong><br>${dataQualityCounts.missingMemo}</div>
            </div>
          </div>

<div class="panel">
  <h2>AI Summary</h2>

  <p style="white-space: pre-wrap;">
    ${escapeHtml(aiSummary)}
  </p>

  <a class="button" href="/bills?ai=true">Generate AI Summary</a>
  <a class="button" href="/bills">Clear</a>
</div>

          <div class="panel">
            <h2>Top 3 Recommended Actions</h2>
            <ol>
              ${unpaidBills.slice(0, 3).map((bill) => `
                <li>
                  <strong>${escapeHtml(bill.vendorName)}</strong>
                  — ${formatMoney(bill.balance, bill.currency)}
                  — ${escapeHtml(bill.recommendedAction)}
                  — Score: ${escapeHtml(String(bill.priorityScore))}
                  — ${escapeHtml(bill.decisionReason)}
                </li>
              `).join('') || '<li>No unpaid bills found.</li>'}
            </ol>
          </div>

          <div class="panel">
            <h2>Alerts</h2>
            ${renderAlerts(alerts)}
          </div>

          <div class="panel">
            <h2>Payment Behavior Layer</h2>
            <div class="section-grid">
              ${services.paymentData.available && paymentMetrics
                ? `
                  <div class="kpi-card"><strong>Average Days to Pay</strong><br>${escapeHtml(String(paymentMetrics.averageDaysToPay ?? 'N/A'))}</div>
                  <div class="kpi-card"><strong>Late Payment Rate</strong><br>${escapeHtml(String(paymentMetrics.latePaymentRate ?? 'N/A'))}%</div>
                  <div class="kpi-card"><strong>Payments Analyzed</strong><br>${escapeHtml(String(paymentMetrics.analyzedPayments ?? 'N/A'))}</div>
                  <div class="kpi-card"><strong>Vendors Tracked</strong><br>${escapeHtml(String(paymentMetrics.vendorsTracked ?? 'N/A'))}</div>
                `
                : renderUnavailableCard('Payment History', services.paymentData.reason)
              }
            </div>
          </div>

          <div class="panel">
            <h2>Financial Health Layer</h2>
            <div class="section-grid">
              ${services.statementData.available && statementMetrics
                ? `
                  <div class="kpi-card"><strong>Current Ratio</strong><br>${escapeHtml(String(statementMetrics.currentRatio ?? 'N/A'))}</div>
                  <div class="kpi-card"><strong>Working Capital</strong><br>${statementMetrics.workingCapital === null ? 'N/A' : formatMoney(statementMetrics.workingCapital)}</div>
                  <div class="kpi-card"><strong>Net Income</strong><br>${statementMetrics.netIncome === null ? 'N/A' : formatMoney(statementMetrics.netIncome)}</div>
                  <div class="kpi-card"><strong>Health Label</strong><br>${escapeHtml(String(statementMetrics.financialHealthLabel ?? 'N/A'))}</div>
                `
                : renderUnavailableCard('Statements', services.statementData.reason)
              }
            </div>
          </div>

          <div class="panel">
            <h2>Liquidity Layer</h2>
            <div class="section-grid">
              ${services.bankData.available && bankMetrics
                ? `
                  <div class="kpi-card"><strong>Available Cash</strong><br>${formatMoney(bankMetrics.availableCash)}</div>
                  <div class="kpi-card"><strong>Cash Coverage of Pay Now Bills</strong><br>${escapeHtml(String(bankMetrics.cashCoveragePayNow ?? 'N/A'))}%</div>
                  <div class="kpi-card"><strong>Cash Coverage of Pay Now + Pay Soon</strong><br>${escapeHtml(String(bankMetrics.cashCoveragePayNowSoon ?? 'N/A'))}%</div>
                  <div class="kpi-card"><strong>Bank Accounts Found</strong><br>${escapeHtml(String(bankMetrics.bankAccountCount ?? 'N/A'))}</div>
                `
                : renderUnavailableCard('Bank / Cash', services.bankData.reason)
              }
            </div>
          </div>

          <div class="panel">
            <h2>Anomaly / Control Layer</h2>
            <div class="section-grid">
              <div class="kpi-card"><strong>Duplicate Warnings</strong><br>${unpaidBills.filter((b) => b.anomalyFlags.some((f) => normalizeText(f).includes('duplicate'))).length}</div>
              <div class="kpi-card"><strong>Abnormal Amount Flags</strong><br>${unpaidBills.filter((b) => b.anomalyFlags.includes('Amount is unusually high vs vendor history')).length}</div>
              <div class="kpi-card"><strong>Review Bucket</strong><br>${actionCounts.review}</div>
            </div>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Action</th>
                  <th>Risk</th>
                  <th>Score</th>
                  <th>Vendor Category</th>
                  <th>Critical Vendor</th>
                  <th>Vendor</th>
                  <th>Bill No.</th>
                  <th>Bill Date</th>
                  <th>Due Date</th>
                  <th>Days Until Due</th>
                  <th>Is Overdue</th>
                  <th>Aging Bucket</th>
                  <th>Amount Bucket</th>
                  <th>Original Amount</th>
                  <th>Amount Paid</th>
                  <th>Balance</th>
                  <th>Payment Status</th>
                  <th>Terms</th>
                  <th>Category</th>
                  <th>Account</th>
                  <th>Location/Class</th>
                  <th>Memo</th>
                  <th>Urgency Drivers</th>
                  <th>Anomaly Flags</th>
                  <th>Data Quality Flags</th>
                  <th>Rule Hits</th>
                  <th>Decision Reason</th>
                  <th>Explanation</th>
                </tr>
              </thead>
              <tbody>
                ${rows || '<tr><td colspan="29">No unpaid bills found.</td></tr>'}
              </tbody>
            </table>
          </div>
<a class="button" href="/bills?refresh=true">Refresh Data</a>
          <a class="button" href="/bills/download">Download CSV</a>
          <a class="button" href="/">Back Home</a>
        </body>
      </html>
    `);
  } catch (err) {
    syncStatus = 'Failed';
    res.status(500).send(`<pre>${escapeHtml(err.response?.data ? JSON.stringify(err.response.data, null, 2) : err.message)}</pre>`);
  }
});

// ======================================================
// CSV DOWNLOAD
// ======================================================

app.get('/bills/download', async (req, res) => {
  try {
    if (!accessToken || !realmId) {
      return res.status(400).send('Not connected to QuickBooks.');
    }

const forceRefresh = req.query.refresh === 'true';
const rawBills = await getBillsFromQuickBooks(forceRefresh);
    const services = await buildAllServices(rawBills);
    const { unpaidBills } = buildDashboardData(rawBills, services);

    const headers = [
      'Rank',
      'Action',
      'Risk',
      'Score',
      'Vendor Category',
      'Critical Vendor',
      'Vendor',
      'Bill ID',
      'Bill No.',
      'Vendor ID',
      'Bill Date',
      'Due Date',
      'Days Until Due',
      'Is Overdue',
      'Aging Bucket',
      'Amount Bucket',
      'Original Amount',
      'Amount Paid',
      'Balance',
      'Currency',
      'Payment Status',
      'Terms',
      'Category Summary',
      'Account',
      'Location / Class',
      'Memo',
      'Urgency Drivers',
      'Anomaly Flags',
      'Data Quality Flags',
      'Rule Hits',
      'Decision Reason',
      'Explanation',
      'Vendor Avg Days To Pay',
      'Vendor Late Payment Rate',
    ];

    const rows = unpaidBills.map((bill) => [
      bill.decisionRank,
      bill.recommendedAction,
      bill.riskLevel,
      bill.priorityScore,
      bill.vendorCategory,
      bill.isCriticalVendor,
      bill.vendorName,
      bill.billId,
      bill.billNo,
      bill.vendorId,
      bill.billDate,
      bill.dueDate || 'No Due Date',
      bill.daysUntilDue ?? '',
      bill.isOverdue,
      bill.agingBucket,
      bill.amountBucket,
      bill.originalAmount,
      bill.amountPaid,
      bill.balance,
      bill.currency,
      bill.paymentStatus,
      bill.terms,
      bill.categorySummary,
      bill.account,
      bill.locationClass,
      bill.memo,
      bill.urgencyDrivers.join('; '),
      bill.anomalyFlags.join('; '),
      bill.dataQualityFlags.join('; '),
      bill.ruleHits.map((hit) => `${hit.label} (+${hit.points}) [${hit.group}]`).join('; '),
      bill.decisionReason,
      bill.explanationText,
      bill.paymentBehaviorSnapshot?.averageDaysToPay ?? '',
      bill.paymentBehaviorSnapshot?.latePaymentRate ?? '',
    ]);

    const csv = [
      headers.map(csvEscape).join(','),
      ...rows.map((row) => row.map(csvEscape).join(',')),
    ].join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="finance-blueprint-step9.csv"');
    res.send(csv);
  } catch (err) {
    res.status(500).send(err.response?.data ? JSON.stringify(err.response.data, null, 2) : err.message);
  }
});

// ======================================================
// SERVER
// ======================================================
app.get('/test', (req, res) => {
  res.send('Server works');
});
app.listen(PORT, () => {
  console.log(`Open http://localhost:${PORT}`);
});
