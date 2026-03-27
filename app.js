require('dotenv').config();

const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const COMPANY_ID = process.env.COMPANY_ID || 'client-1';

const QB_CLIENT_ID = process.env.QB_CLIENT_ID || '';
const QB_CLIENT_SECRET = process.env.QB_CLIENT_SECRET || '';
const QB_REDIRECT_URI = process.env.QB_REDIRECT_URI || `http://localhost:${PORT}/callback`;
const QB_ENV = process.env.QB_ENV || 'sandbox';
const QB_OAUTH_STATE = process.env.QB_OAUTH_STATE || 'final-core-blueprint-state';

const QB_BASE_URL =
  QB_ENV === 'production'
    ? 'https://quickbooks.api.intuit.com'
    : 'https://sandbox-quickbooks.api.intuit.com';

const CACHE_TTL_MS = 60 * 1000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }
    : false,
});

let accessToken = '';
let refreshToken = '';
let realmId = '';
let tokenExpiresAt = null;
let refreshExpiresAt = null;
let lastSyncTime = null;
let syncStatus = 'Not synced';

const cache = {
  bills: { data: null, fetchedAt: 0 },
  invoices: { data: null, fetchedAt: 0 },
  accounts: { data: null, fetchedAt: 0 },
  customers: { data: null, fetchedAt: 0 },
  billPayments: { data: null, fetchedAt: 0 },
  reports: { pnl: null, bs: null, fetchedAt: 0 },
};

const CONFIG = {
  thresholds: {
    dueSoonDays: 3,
    dueWeekDays: 7,
    due14Days: 14,
    overdueHighDays: 14,
    overdueCriticalDays: 30,
    mediumBillAmount: 1000,
    largeBillAmount: 2500,
    highInvoiceAmount: 5000,
    concentrationMediumPct: 20,
    concentrationHighPct: 35,
    healthyCurrentRatio: 1.5,
    weakCurrentRatio: 1.0,
    healthyCashCoveragePct: 150,
    weakCashCoveragePct: 100,
    abnormalAmountVariancePct: 40,
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
  criticalVendorCategories: new Set([
    'Utilities',
    'Rent / Lease',
    'Insurance',
    'Payroll Related',
    'Inventory / Materials',
  ]),
};

const INDUSTRY_TEMPLATES = {
  general: {
    label: 'General SMB',
    focus: ['AP', 'AR', 'Cash', 'Financial Health', 'Controls'],
    extraMetrics: [],
    intakeFields: ['locations', 'employeeCount', 'usesInventory', 'usesProjects', 'collectsDeposits'],
    nextBuildSuggestions: ['cash flow forecasting', 'custom rules', 'owner reporting'],
  },
  trade_install: {
    label: 'Trade / Install',
    focus: ['AP', 'AR', 'Cash', 'Financial Health', 'Controls', 'Materials', 'Job Billing'],
    extraMetrics: ['inventoryCogsMismatch', 'materialsExposure'],
    intakeFields: ['usesProjects', 'collectsDeposits', 'usesSubcontractors', 'tracksJobCosting'],
    nextBuildSuggestions: ['job costing', 'WIP', 'deposits', 'crew profitability'],
  },
  retail: {
    label: 'Retail',
    focus: ['AP', 'AR', 'Cash', 'Financial Health', 'Controls', 'Inventory'],
    extraMetrics: ['inventoryCogsMismatch'],
    intakeFields: ['usesInventory', 'locationCount', 'tracksClassesOrLocations'],
    nextBuildSuggestions: ['SKU profitability', 'inventory turns', 'reorder logic'],
  },
  professional_services: {
    label: 'Professional Services',
    focus: ['AR', 'Cash', 'Collections', 'Financial Health', 'Controls'],
    extraMetrics: [],
    intakeFields: ['billingModel', 'employeeCount', 'tracksTime', 'retainerModel'],
    nextBuildSuggestions: ['utilization', 'billable hours', 'staff margin'],
  },
};

function round2(num) {
  return Math.round((Number(num || 0) + Number.EPSILON) * 100) / 100;
}

function safeDivide(a, b) {
  if (!b) return null;
  return round2(a / b);
}

function normalizeText(value, fallback = 'N/A') {
  if (value === null || value === undefined) return fallback;
  const str = String(value).trim();
  return str || fallback;
}

function lower(value) {
  return String(value || '').trim().toLowerCase();
}

function parseDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function dateOnlyString(value) {
  const d = parseDate(value);
  return d ? d.toISOString().slice(0, 10) : null;
}

function todayDateOnly() {
  const d = new Date();
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function daysBetweenDates(start, end) {
  const s = parseDate(start);
  const e = parseDate(end);
  if (!s || !e) return null;
  const s0 = new Date(s.getFullYear(), s.getMonth(), s.getDate());
  const e0 = new Date(e.getFullYear(), e.getMonth(), e.getDate());
  return Math.round((e0 - s0) / 86400000);
}

function getDaysUntilDue(dueDate) {
  if (!dueDate) return null;
  const d = parseDate(dueDate);
  if (!d) return null;
  const due = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  return Math.round((due - todayDateOnly()) / 86400000);
}

function agingBucket(daysUntilDue) {
  if (daysUntilDue === null) return 'No Due Date';
  if (daysUntilDue < 0) return 'Overdue';
  if (daysUntilDue === 0) return 'Due Today';
  if (daysUntilDue <= 3) return 'Due 1-3 Days';
  if (daysUntilDue <= 7) return 'Due 4-7 Days';
  if (daysUntilDue <= 14) return 'Due 8-14 Days';
  return 'Due Later';
}

function average(values) {
  if (!values.length) return null;
  return round2(values.reduce((a, b) => a + Number(b || 0), 0) / values.length);
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) return round2((sorted[mid - 1] + sorted[mid]) / 2);
  return round2(sorted[mid]);
}

function isCacheFresh(name) {
  return cache[name] && cache[name].data && (Date.now() - cache[name].fetchedAt < CACHE_TTL_MS);
}

function isReportCacheFresh() {
  return cache.reports.fetchedAt && (Date.now() - cache.reports.fetchedAt < CACHE_TTL_MS);
}

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS qb_connections (
      company_id TEXT PRIMARY KEY,
      realm_id TEXT NOT NULL,
      access_token TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      token_expires_at TIMESTAMP,
      refresh_expires_at TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS blueprint_snapshots (
      id SERIAL PRIMARY KEY,
      company_id TEXT NOT NULL,
      snapshot_hash TEXT NOT NULL,
      total_ap NUMERIC DEFAULT 0,
      overdue_ap NUMERIC DEFAULT 0,
      total_ar NUMERIC DEFAULT 0,
      overdue_ar NUMERIC DEFAULT 0,
      available_cash NUMERIC DEFAULT 0,
      current_ratio NUMERIC,
      readiness_score NUMERIC,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS company_intake_profiles (
      company_id TEXT PRIMARY KEY,
      industry TEXT DEFAULT 'general',
      payload JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
}

async function saveConnection() {
  await pool.query(
    `INSERT INTO qb_connections
      (company_id, realm_id, access_token, refresh_token, token_expires_at, refresh_expires_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
     ON CONFLICT (company_id)
     DO UPDATE SET
       realm_id = EXCLUDED.realm_id,
       access_token = EXCLUDED.access_token,
       refresh_token = EXCLUDED.refresh_token,
       token_expires_at = EXCLUDED.token_expires_at,
       refresh_expires_at = EXCLUDED.refresh_expires_at,
       updated_at = CURRENT_TIMESTAMP`,
    [COMPANY_ID, realmId, accessToken, refreshToken, tokenExpiresAt, refreshExpiresAt]
  );
}

async function loadConnection() {
  const result = await pool.query(
    `SELECT * FROM qb_connections WHERE company_id = $1 LIMIT 1`,
    [COMPANY_ID]
  );

  const row = result.rows[0];
  if (!row) return null;

  realmId = row.realm_id;
  accessToken = row.access_token;
  refreshToken = row.refresh_token;
  tokenExpiresAt = row.token_expires_at;
  refreshExpiresAt = row.refresh_expires_at;

  return row;
}

async function saveIntakeProfile({ companyId = COMPANY_ID, industry = 'general', payload = {} }) {
  await pool.query(
    `INSERT INTO company_intake_profiles (company_id, industry, payload, updated_at)
     VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
     ON CONFLICT (company_id)
     DO UPDATE SET industry = EXCLUDED.industry, payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP`,
    [companyId, industry, JSON.stringify(payload)]
  );
}

async function loadIntakeProfile(companyId = COMPANY_ID) {
  const result = await pool.query(
    `SELECT company_id, industry, payload, updated_at
     FROM company_intake_profiles
     WHERE company_id = $1
     LIMIT 1`,
    [companyId]
  );
  return result.rows[0] || null;
}

async function saveSnapshotIfChanged(payload) {
  const compact = {
    totalAP: round2(payload.payables.totalAP || 0),
    overdueAP: round2(payload.payables.overdueAP || 0),
    totalAR: round2(payload.receivables.totalAR || 0),
    overdueAR: round2(payload.receivables.overdueAR || 0),
    availableCash: round2(payload.liquidity.availableCash || 0),
    currentRatio: payload.financialHealth.currentRatio,
    readinessScore: payload.readiness.score,
  };

  const snapshotHash = crypto
    .createHash('sha256')
    .update(JSON.stringify(compact))
    .digest('hex');

  const existing = await pool.query(
    `SELECT id
     FROM blueprint_snapshots
     WHERE company_id = $1 AND snapshot_hash = $2
     ORDER BY created_at DESC
     LIMIT 1`,
    [COMPANY_ID, snapshotHash]
  );

  if (existing.rowCount > 0) return false;

  await pool.query(
    `INSERT INTO blueprint_snapshots
      (company_id, snapshot_hash, total_ap, overdue_ap, total_ar, overdue_ar, available_cash, current_ratio, readiness_score)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [
      COMPANY_ID,
      snapshotHash,
      compact.totalAP,
      compact.overdueAP,
      compact.totalAR,
      compact.overdueAR,
      compact.availableCash,
      compact.currentRatio,
      compact.readinessScore,
    ]
  );

  return true;
}

async function getSnapshotHistory(limit = 12) {
  const result = await pool.query(
    `SELECT company_id, total_ap, overdue_ap, total_ar, overdue_ar, available_cash, current_ratio, readiness_score, created_at
     FROM blueprint_snapshots
     WHERE company_id = $1
     ORDER BY created_at DESC
     LIMIT $2`,
    [COMPANY_ID, limit]
  );

  return result.rows;
}

async function refreshQuickBooksAccessToken() {
  if (!refreshToken) throw new Error('QuickBooks refresh token missing');

  const response = await axios.post(
    'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
    `grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshToken)}`,
    {
      headers: {
        Authorization: 'Basic ' + Buffer.from(`${QB_CLIENT_ID}:${QB_CLIENT_SECRET}`).toString('base64'),
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
      },
    }
  );

  accessToken = response.data.access_token;
  refreshToken = response.data.refresh_token || refreshToken;
  tokenExpiresAt = response.data.expires_in ? new Date(Date.now() + response.data.expires_in * 1000) : null;
  refreshExpiresAt = response.data.x_refresh_token_expires_in ? new Date(Date.now() + response.data.x_refresh_token_expires_in * 1000) : refreshExpiresAt;

  await saveConnection();
  return accessToken;
}

async function qbGet(path, params = {}, attempt = 0) {
  if (!accessToken || !realmId) throw new Error('QuickBooks not connected');

  try {
    return await axios.get(`${QB_BASE_URL}${path}`, {
      params,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: 'application/json',
      },
    });
  } catch (err) {
    if (err.response?.status === 401 && attempt === 0 && refreshToken) {
      await refreshQuickBooksAccessToken();
      return qbGet(path, params, attempt + 1);
    }
    throw err;
  }
}

async function qbQuery(sql) {
  return qbGet(`/v3/company/${realmId}/query`, { query: sql });
}

async function getCachedList(name, sql, responseKey) {
  if (isCacheFresh(name)) return cache[name].data;

  const response = await qbQuery(sql);
  const data = response.data.QueryResponse?.[responseKey] || [];

  cache[name] = {
    data,
    fetchedAt: Date.now(),
  };

  return data;
}

async function getBills(forceRefresh = false) {
  if (forceRefresh) cache.bills = { data: null, fetchedAt: 0 };
  return getCachedList('bills', 'SELECT * FROM Bill MAXRESULTS 1000', 'Bill');
}

async function getInvoices(forceRefresh = false) {
  if (forceRefresh) cache.invoices = { data: null, fetchedAt: 0 };
  return getCachedList('invoices', 'SELECT * FROM Invoice MAXRESULTS 1000', 'Invoice');
}

async function getAccounts(forceRefresh = false) {
  if (forceRefresh) cache.accounts = { data: null, fetchedAt: 0 };
  return getCachedList('accounts', 'SELECT * FROM Account MAXRESULTS 1000', 'Account');
}

async function getCustomers(forceRefresh = false) {
  if (forceRefresh) cache.customers = { data: null, fetchedAt: 0 };
  return getCachedList('customers', 'SELECT * FROM Customer MAXRESULTS 1000', 'Customer');
}

async function getBillPayments(forceRefresh = false) {
  if (forceRefresh) cache.billPayments = { data: null, fetchedAt: 0 };
  return getCachedList('billPayments', 'SELECT * FROM BillPayment MAXRESULTS 500', 'BillPayment');
}

async function getReports(forceRefresh = false) {
  if (!forceRefresh && isReportCacheFresh()) {
    return { pnl: cache.reports.pnl, bs: cache.reports.bs };
  }

  const [pnlRes, bsRes] = await Promise.all([
    qbGet(`/v3/company/${realmId}/reports/ProfitAndLoss`, {
      accounting_method: 'Accrual',
      summarize_column_by: 'Total',
    }).catch(() => ({ data: null })),
    qbGet(`/v3/company/${realmId}/reports/BalanceSheet`, {
      accounting_method: 'Accrual',
      summarize_column_by: 'Total',
    }).catch(() => ({ data: null })),
  ]);

  cache.reports = {
    pnl: pnlRes.data || null,
    bs: bsRes.data || null,
    fetchedAt: Date.now(),
  };

  return { pnl: cache.reports.pnl, bs: cache.reports.bs };
}

function flattenReportRows(rows = [], out = []) {
  for (const row of rows) {
    if (row?.Summary?.ColData?.length) out.push(row);
    if (row?.Rows?.Row?.length) flattenReportRows(row.Rows.Row, out);
  }
  return out;
}

function findReportValue(report, labels) {
  if (!report?.Rows?.Row) return null;

  const rows = flattenReportRows(report.Rows.Row, []);
  const targets = labels.map(lower);

  for (const row of rows) {
    const label = lower(row?.Summary?.ColData?.[0]?.value || '');
    if (!label) continue;

    if (targets.some((t) => label.includes(t))) {
      const cell = row.Summary.ColData.find((c, idx) => idx > 0 && c?.value !== undefined);
      const value = Number(String(cell?.value || '').replace(/,/g, ''));
      if (Number.isFinite(value)) return value;
    }
  }

  return null;
}

function classifyVendorCategory({ vendorName = '', accountName = '', categorySummary = '', memo = '' }) {
  const text = `${lower(vendorName)} ${lower(accountName)} ${lower(categorySummary)} ${lower(memo)}`;

  if (/(electric|power|water|utility|gas|internet|wifi|telecom)/.test(text)) return 'Utilities';
  if (/(rent|lease|landlord|property)/.test(text)) return 'Rent / Lease';
  if (/(insurance)/.test(text)) return 'Insurance';
  if (/(payroll|adp|paychex|gusto|wages|salary)/.test(text)) return 'Payroll Related';
  if (/(supply|supplies|inventory|materials|wholesale|parts|glass|hardware|aluminum|lumber)/.test(text)) return 'Inventory / Materials';
  if (/(repair|repairs|maintenance|service|contractor)/.test(text)) return 'Maintenance / Repair';
  if (/(marketing|advertising|promo|software|subscription|saas)/.test(text)) return 'Discretionary';

  return 'General';
}

function getLineCategorySummary(lines = []) {
  if (!Array.isArray(lines) || !lines.length) return 'N/A';

  const values = lines.map((line) => {
    return line?.AccountBasedExpenseLineDetail?.AccountRef?.name
      || line?.ItemBasedExpenseLineDetail?.ItemRef?.name
      || line?.Description
      || null;
  }).filter(Boolean);

  return values.length ? [...new Set(values)].join(', ') : 'N/A';
}

function getPrimaryAccountName(lines = []) {
  if (!Array.isArray(lines)) return 'N/A';

  for (const line of lines) {
    const name = line?.AccountBasedExpenseLineDetail?.AccountRef?.name;
    if (name) return name;
  }

  return 'N/A';
}

function normalizeBill(raw) {
  const vendorName = normalizeText(raw?.VendorRef?.name, 'Unknown Vendor');
  const accountName = normalizeText(getPrimaryAccountName(raw?.Line), 'N/A');
  const categorySummary = normalizeText(getLineCategorySummary(raw?.Line), 'N/A');
  const memo = normalizeText(raw?.PrivateNote || raw?.Memo, '');
  const originalAmount = round2(raw?.TotalAmt || 0);
  const balance = round2(raw?.Balance || 0);
  const amountPaid = round2(Math.max(0, originalAmount - balance));
  const dueDate = dateOnlyString(raw?.DueDate);
  const billDate = dateOnlyString(raw?.TxnDate);
  const terms = normalizeText(raw?.SalesTermRef?.name || raw?.TermsRef?.name, 'N/A');
  const billNo = normalizeText(raw?.DocNumber, 'N/A');
  const daysUntilDue = getDaysUntilDue(dueDate);

  return {
    billId: normalizeText(raw?.Id, 'N/A'),
    vendorName,
    billNo,
    billDate,
    dueDate,
    accountName,
    categorySummary,
    memo,
    originalAmount,
    balance,
    amountPaid,
    terms,
    daysUntilDue,
    isOverdue: daysUntilDue !== null && daysUntilDue < 0,
    agingBucket: agingBucket(daysUntilDue),
    vendorCategory: classifyVendorCategory({ vendorName, accountName, categorySummary, memo }),
  };
}

function normalizeInvoice(raw) {
  const customerName = normalizeText(raw?.CustomerRef?.name, 'Unknown Customer');
  const totalAmount = round2(raw?.TotalAmt || 0);
  const balance = round2(raw?.Balance || 0);
  const dueDate = dateOnlyString(raw?.DueDate);
  const invoiceDate = dateOnlyString(raw?.TxnDate);
  const daysUntilDue = getDaysUntilDue(dueDate);

  return {
    invoiceId: normalizeText(raw?.Id, 'N/A'),
    invoiceNo: normalizeText(raw?.DocNumber, 'N/A'),
    customerName,
    invoiceDate,
    dueDate,
    totalAmount,
    balance,
    daysUntilDue,
    isOverdue: daysUntilDue !== null && daysUntilDue < 0,
    agingBucket: agingBucket(daysUntilDue),
  };
}

function buildPayablesKpis(openBills) {
  const totalAP = round2(openBills.reduce((sum, b) => sum + Number(b.balance || 0), 0));
  const overdueBills = openBills.filter((b) => b.isOverdue);
  const overdueAP = round2(overdueBills.reduce((sum, b) => sum + Number(b.balance || 0), 0));

  const vendorTotals = {};
  for (const bill of openBills) {
    vendorTotals[bill.vendorName] = round2((vendorTotals[bill.vendorName] || 0) + Number(bill.balance || 0));
  }

  const sortedVendorTotals = Object.entries(vendorTotals)
    .map(([vendor, amount]) => ({ vendor, amount, pctOfAP: totalAP ? round2((amount / totalAP) * 100) : 0 }))
    .sort((a, b) => b.amount - a.amount);

  const topVendorPct = sortedVendorTotals[0]?.pctOfAP || 0;
  const vendorConcentrationRisk =
    topVendorPct >= CONFIG.thresholds.concentrationHighPct ? 'High'
      : topVendorPct >= CONFIG.thresholds.concentrationMediumPct ? 'Medium'
        : 'Low';

  return {
    totalAP,
    overdueAP,
    openBillCount: openBills.length,
    overdueBillCount: overdueBills.length,
    vendorConcentrationRisk,
    topVendors: sortedVendorTotals.slice(0, 10),
  };
}

function buildReceivablesKpis(openInvoices) {
  const totalAR = round2(openInvoices.reduce((sum, i) => sum + Number(i.balance || 0), 0));
  const overdueInvoices = openInvoices.filter((i) => i.isOverdue);
  const overdueAR = round2(overdueInvoices.reduce((sum, i) => sum + Number(i.balance || 0), 0));

  const customerTotals = {};
  for (const invoice of openInvoices) {
    customerTotals[invoice.customerName] = round2((customerTotals[invoice.customerName] || 0) + Number(invoice.balance || 0));
  }

  const topCustomers = Object.entries(customerTotals)
    .map(([customer, amount]) => ({ customer, amount, pctOfAR: totalAR ? round2((amount / totalAR) * 100) : 0 }))
    .sort((a, b) => b.amount - a.amount)
    .slice(0, 10);

  return {
    totalAR,
    overdueAR,
    openInvoiceCount: openInvoices.length,
    overdueInvoiceCount: overdueInvoices.length,
    topCustomers,
  };
}

function buildLiquidity(rawAccounts, payablesKpis) {
  const accounts = Array.isArray(rawAccounts) ? rawAccounts : [];
  const bankTypes = new Set(['Bank', 'Other Current Asset']);
  const bankAccounts = accounts.filter((a) => bankTypes.has(a?.AccountType));
  const availableCash = round2(bankAccounts.reduce((sum, a) => sum + Number(a?.CurrentBalance || 0), 0));
  const cashCoveragePct = payablesKpis.totalAP ? round2((availableCash / payablesKpis.totalAP) * 100) : null;

  return {
    availableCash,
    cashCoveragePct,
    bankAccountCount: bankAccounts.length,
  };
}

function buildFinancialHealth(reports) {
  const pnl = reports?.pnl || null;
  const bs = reports?.bs || null;

  const totalIncome = findReportValue(pnl, ['total income', 'income']);
  const grossProfit = findReportValue(pnl, ['gross profit']);
  const netIncome = findReportValue(pnl, ['net income']);
  const currentAssets = findReportValue(bs, ['total current assets']);
  const currentLiabilities = findReportValue(bs, ['total current liabilities']);
  const totalAssets = findReportValue(bs, ['total assets']);
  const totalLiabilities = findReportValue(bs, ['total liabilities']);

  return {
    totalIncome,
    grossProfit,
    netIncome,
    grossMarginPct: totalIncome ? round2((grossProfit / totalIncome) * 100) : null,
    netMarginPct: totalIncome ? round2((netIncome / totalIncome) * 100) : null,
    currentAssets,
    currentLiabilities,
    currentRatio: safeDivide(currentAssets, currentLiabilities),
    totalAssets,
    totalLiabilities,
  };
}

function buildPaymentBehavior(rawBills, rawBillPayments) {
  const bills = Array.isArray(rawBills) ? rawBills : [];
  const billPayments = Array.isArray(rawBillPayments) ? rawBillPayments : [];

  const paymentMap = new Map();
  for (const payment of billPayments) {
    const lines = payment?.Line || [];
    for (const line of lines) {
      const linked = line?.LinkedTxn || [];
      for (const txn of linked) {
        if (txn?.TxnType === 'Bill' && txn?.TxnId) {
          if (!paymentMap.has(txn.TxnId)) paymentMap.set(txn.TxnId, []);
          paymentMap.get(txn.TxnId).push(payment?.TxnDate || null);
        }
      }
    }
  }

  const vendorStats = {};
  for (const rawBill of bills) {
    const vendorName = normalizeText(rawBill?.VendorRef?.name, 'Unknown Vendor');
    const billId = normalizeText(rawBill?.Id, 'N/A');
    const billDate = rawBill?.TxnDate || null;
    const paidDates = paymentMap.get(billId) || [];
    if (!paidDates.length || !billDate) continue;

    const firstPaidDate = paidDates[0];
    const daysToPay = daysBetweenDates(billDate, firstPaidDate);
    if (daysToPay === null) continue;

    if (!vendorStats[vendorName]) vendorStats[vendorName] = { paymentDays: [] };
    vendorStats[vendorName].paymentDays.push(daysToPay);
  }

  Object.keys(vendorStats).forEach((vendor) => {
    const paymentDays = vendorStats[vendor].paymentDays;
    vendorStats[vendor] = {
      averageDaysToPay: average(paymentDays),
      medianDaysToPay: median(paymentDays),
      sampleSize: paymentDays.length,
    };
  });

  return {
    available: true,
    metrics: {
      vendorsTracked: Object.keys(vendorStats).length,
    },
    vendorStats,
  };
}

function detectBillDuplicates(openBills) {
  const duplicateMap = new Map();
  const byBillNo = new Map();
  const byVendorAmountDate = new Map();

  for (const bill of openBills) {
    if (bill.billNo && bill.billNo !== 'N/A') {
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

function buildVendorAmountBenchmarks(openBills) {
  const map = new Map();
  for (const bill of openBills) {
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

function buildControls(openBills, openInvoices, financialHealth) {
  const duplicateMap = detectBillDuplicates(openBills);
  const vendorBenchmarks = buildVendorAmountBenchmarks(openBills);

  const missingBillNoCount = openBills.filter((b) => b.billNo === 'N/A').length;
  const missingDueDateCount = openBills.filter((b) => !b.dueDate).length;
  const missingInvoiceNoCount = openInvoices.filter((i) => i.invoiceNo === 'N/A').length;
  const negativeCurrentRatio = financialHealth.currentRatio !== null && financialHealth.currentRatio < 1;

  return {
    duplicateBillCount: duplicateMap.size,
    missingBillNoCount,
    missingDueDateCount,
    missingInvoiceNoCount,
    negativeCurrentRatio,
    duplicateMap,
    vendorBenchmarks,
  };
}

function getBillDecisionReason(urgencyDrivers, anomalyFlags, dataQualityFlags) {
  const merged = [...urgencyDrivers, ...anomalyFlags, ...dataQualityFlags];
  return merged.length ? merged.slice(0, 2).join(' + ') : 'No immediate concern';
}

function applyRulesToBill(bill, context) {
  const { duplicateWarnings = [], paymentBehavior = null, financialHealth = null, liquidity = null, vendorBenchmarks = {} } = context || {};

  const urgencyDrivers = [];
  const dataQualityFlags = [];
  const anomalyFlags = [];
  let score = 0;

  const addScore = (points, label, group = 'urgency') => {
    score += points;
    if (group === 'urgency') urgencyDrivers.push(label);
    if (group === 'data') dataQualityFlags.push(label);
    if (group === 'anomaly') anomalyFlags.push(label);
  };

  if (bill.isOverdue) {
    const extraDays = Math.min(Math.abs(bill.daysUntilDue) * CONFIG.weights.overduePerDay, CONFIG.weights.overdueMaxExtra);
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

  if (bill.originalAmount >= CONFIG.thresholds.largeBillAmount) addScore(CONFIG.weights.largeAmount, 'Large bill');
  else if (bill.originalAmount >= CONFIG.thresholds.mediumBillAmount) addScore(CONFIG.weights.mediumAmount, 'Medium bill');

  if (CONFIG.criticalVendorCategories.has(bill.vendorCategory)) {
    addScore(CONFIG.weights.criticalVendor, 'Critical vendor category');
  }

  if (duplicateWarnings.length) {
    addScore(CONFIG.weights.duplicateSuspected, 'Possible duplicate detected', 'anomaly');
  }

  if (!bill.dueDate) addScore(CONFIG.weights.missingDueDate, 'Missing due date', 'data');
  if (!bill.billNo || bill.billNo === 'N/A') addScore(CONFIG.weights.missingBillNo, 'Missing invoice number', 'data');
  if (!bill.terms || bill.terms === 'N/A') addScore(CONFIG.weights.missingTerms, 'Missing terms', 'data');
  if (!bill.memo) addScore(CONFIG.weights.missingMemo, 'Missing memo', 'data');

  const vendorBenchmark = vendorBenchmarks[bill.vendorName];
  if (vendorBenchmark?.medianAmount && vendorBenchmark.sampleSize >= 3) {
    const variancePct = Math.abs((bill.originalAmount - vendorBenchmark.medianAmount) / vendorBenchmark.medianAmount) * 100;
    if (variancePct >= CONFIG.thresholds.abnormalAmountVariancePct) {
      addScore(CONFIG.weights.abnormalAmount, 'Amount is unusually high vs vendor history', 'anomaly');
    }
  }

  const vendorPayStats = paymentBehavior?.vendorStats?.[bill.vendorName];
  if (vendorPayStats?.medianDaysToPay !== null && vendorPayStats?.sampleSize >= 3 && bill.daysUntilDue !== null) {
    if (bill.daysUntilDue < -7) addScore(CONFIG.weights.abnormalPaymentTiming, 'Abnormal payment timing');
  }

  if (financialHealth?.currentRatio !== null) {
    if (financialHealth.currentRatio < CONFIG.thresholds.weakCurrentRatio) addScore(CONFIG.weights.weakFinancials, 'Weak financial health context');
    else if (financialHealth.currentRatio >= CONFIG.thresholds.healthyCurrentRatio) addScore(CONFIG.weights.healthyFinancials, 'Healthy financial health context');
  }

  if (liquidity?.cashCoveragePct !== null) {
    if (liquidity.cashCoveragePct < CONFIG.thresholds.weakCashCoveragePct) addScore(CONFIG.weights.lowCashCoverage, 'Cash cannot fully cover');
    else if (liquidity.cashCoveragePct < CONFIG.thresholds.healthyCashCoveragePct) addScore(CONFIG.weights.mediumCashCoverage, 'Cash coverage is tight');
  }

  let action = 'Monitor';
  let riskLevel = 'Low';

  if (score >= CONFIG.actionCutoffs.payNowMin) {
    action = 'Pay Now';
    riskLevel = 'High';
  } else if (score >= CONFIG.actionCutoffs.paySoonMin) {
    action = 'Pay Soon';
    riskLevel = 'Medium';
  } else if (score >= CONFIG.actionCutoffs.reviewMin) {
    action = 'Review';
    riskLevel = 'Medium';
  }

  return {
    ...bill,
    priorityScore: round2(score),
    action,
    riskLevel,
    decisionReason: getBillDecisionReason(urgencyDrivers, anomalyFlags, dataQualityFlags),
    urgencyDrivers,
    anomalyFlags,
    dataQualityFlags,
    duplicateWarnings,
  };
}

function buildPayablesPriorities(openBills, liquidity, financialHealth, paymentBehavior, controls) {
  return openBills
    .map((bill) => applyRulesToBill(bill, {
      duplicateWarnings: controls.duplicateMap.get(bill.billId) || [],
      paymentBehavior,
      financialHealth,
      liquidity,
      vendorBenchmarks: controls.vendorBenchmarks,
    }))
    .sort((a, b) => b.priorityScore - a.priorityScore);
}

function buildCollectionsPriorities(openInvoices, receivablesKpis) {
  return openInvoices
    .map((invoice) => {
      let score = 0;
      if (invoice.isOverdue) score += 50 + Math.min(Math.abs(invoice.daysUntilDue || 0) * 2, 20);
      else if (invoice.daysUntilDue === 0) score += 35;
      else if (invoice.daysUntilDue !== null && invoice.daysUntilDue <= 7) score += 15;
      if (invoice.balance >= CONFIG.thresholds.highInvoiceAmount) score += 20;

      const customerTop = receivablesKpis.topCustomers.find((x) => x.customer === invoice.customerName);
      if ((customerTop?.pctOfAR || 0) >= CONFIG.thresholds.concentrationHighPct) score += 10;

      const action = score >= 60 ? 'Collect Now' : score >= 30 ? 'Collect Soon' : 'Monitor';
      return {
        ...invoice,
        priorityScore: round2(score),
        action,
      };
    })
    .sort((a, b) => b.priorityScore - a.priorityScore);
}

function buildReadiness(financialHealth, controls, liquidity, payablesKpis, receivablesKpis, intakeProfile) {
  let score = 100;
  const issues = [];

  if (controls.duplicateBillCount > 0) {
    score -= 15;
    issues.push('Duplicate bill signals detected');
  }
  if (controls.missingBillNoCount > 0) {
    score -= 10;
    issues.push('Bills missing invoice numbers');
  }
  if (controls.missingDueDateCount > 0) {
    score -= 10;
    issues.push('Bills missing due dates');
  }
  if (controls.missingInvoiceNoCount > 0) {
    score -= 5;
    issues.push('Invoices missing document numbers');
  }
  if (financialHealth.currentRatio !== null && financialHealth.currentRatio < CONFIG.thresholds.weakCurrentRatio) {
    score -= 15;
    issues.push('Weak current ratio');
  }
  if (liquidity.cashCoveragePct !== null && liquidity.cashCoveragePct < CONFIG.thresholds.weakCashCoveragePct) {
    score -= 15;
    issues.push('Cash does not cover AP comfortably');
  }
  if (payablesKpis.vendorConcentrationRisk === 'High') {
    score -= 10;
    issues.push('High vendor concentration');
  }
  if ((receivablesKpis.overdueAR || 0) > 0 && (receivablesKpis.totalAR || 0) > 0 && ((receivablesKpis.overdueAR / receivablesKpis.totalAR) * 100) > 35) {
    score -= 10;
    issues.push('Overdue AR is elevated');
  }
  if (!intakeProfile) {
    score -= 10;
    issues.push('Business intake profile not completed');
  }

  score = Math.max(0, round2(score));

  const level = score >= 85 ? 'Ready'
    : score >= 70 ? 'Usable'
      : score >= 50 ? 'Needs Cleanup'
        : 'Needs Intervention';

  return {
    score,
    level,
    issues,
  };
}

function buildAlerts(payablesKpis, receivablesKpis, liquidity, financialHealth, controls) {
  const alerts = [];

  if (payablesKpis.overdueAP > 0) alerts.push({ severity: 'high', code: 'OVERDUE_AP', message: 'There are overdue bills requiring attention.' });
  if (receivablesKpis.overdueAR > 0) alerts.push({ severity: 'medium', code: 'OVERDUE_AR', message: 'There are overdue invoices affecting cash collection.' });
  if (liquidity.cashCoveragePct !== null && liquidity.cashCoveragePct < CONFIG.thresholds.weakCashCoveragePct) alerts.push({ severity: 'high', code: 'LOW_CASH_COVERAGE', message: 'Cash coverage versus AP is weak.' });
  if (financialHealth.currentRatio !== null && financialHealth.currentRatio < CONFIG.thresholds.weakCurrentRatio) alerts.push({ severity: 'high', code: 'WEAK_CURRENT_RATIO', message: 'Current ratio is below target.' });
  if (controls.duplicateBillCount > 0) alerts.push({ severity: 'high', code: 'DUPLICATE_BILLS', message: 'Potential duplicate bills detected.' });

  return alerts;
}

function buildEscalation(readiness, controls, intakeProfile) {
  if (readiness.score >= 85 && controls.duplicateBillCount === 0) {
    return { level: 'Remote Ready', reason: 'Data is clean enough for online use with minimal help.' };
  }
  if (readiness.score >= 60) {
    return { level: 'Remote Cleanup', reason: 'Can likely be standardized remotely with guided fixes.' };
  }
  if (!intakeProfile) {
    return { level: 'Intake Required', reason: 'Need intake profile before deciding deeper escalation.' };
  }
  return { level: 'Controller Review', reason: 'Books or workflows are materially broken and need deeper intervention.' };
}

function buildHistorySummary(rows = []) {
  const points = [...rows].reverse().map((row) => ({
    createdAt: row.created_at,
    totalAP: round2(row.total_ap),
    overdueAP: round2(row.overdue_ap),
    totalAR: round2(row.total_ar),
    overdueAR: round2(row.overdue_ar),
    availableCash: round2(row.available_cash),
    currentRatio: row.current_ratio !== null ? round2(row.current_ratio) : null,
    readinessScore: row.readiness_score !== null ? round2(row.readiness_score) : null,
  }));

  if (points.length < 2) return { points, trend: null };

  const first = points[0];
  const last = points[points.length - 1];

  return {
    points,
    trend: {
      totalAPDelta: round2(last.totalAP - first.totalAP),
      overdueAPDelta: round2(last.overdueAP - first.overdueAP),
      totalARDelta: round2(last.totalAR - first.totalAR),
      overdueARDelta: round2(last.overdueAR - first.overdueAR),
      cashDelta: round2(last.availableCash - first.availableCash),
      readinessDelta: round2((last.readinessScore || 0) - (first.readinessScore || 0)),
    },
  };
}

function buildRemoteIntakeSchema(industry = 'general') {
  const template = INDUSTRY_TEMPLATES[industry] || INDUSTRY_TEMPLATES.general;
  return {
    industry,
    required: [
      'companyType',
      'locationCount',
      'employeeCount',
      'usesInventory',
      'usesProjects',
      'billingTiming',
      'collectsDeposits',
      'payrollSystem',
      'tracksClassesOrLocations',
      'biggestPainPoint',
    ],
    templateSpecific: template.intakeFields,
  };
}

function applyIndustryOverlay(templateKey, blueprint, intakeProfile) {
  const template = INDUSTRY_TEMPLATES[templateKey] || INDUSTRY_TEMPLATES.general;

  const openMaterialBills = blueprint.payables.priorities.filter((b) => b.vendorCategory === 'Inventory / Materials');
  const materialsExposure = round2(openMaterialBills.reduce((sum, b) => sum + Number(b.balance || 0), 0));
  const inventoryCogsMismatch = Boolean(
    intakeProfile?.payload?.usesInventory &&
    blueprint.financialHealth.grossMarginPct !== null &&
    blueprint.financialHealth.grossMarginPct > 80
  );

  return {
    industryTemplate: {
      key: templateKey in INDUSTRY_TEMPLATES ? templateKey : 'general',
      ...template,
    },
    extensionLayer: {
      materialsExposure,
      inventoryCogsMismatch,
      nextBuildSuggestions: template.nextBuildSuggestions,
    },
  };
}

async function buildFinalCoreBlueprint({ forceRefresh = false, industry = 'general' } = {}) {
  if (!accessToken || !realmId) throw new Error('QuickBooks not connected');

  const intakeProfile = await loadIntakeProfile(COMPANY_ID);
  const selectedIndustry = intakeProfile?.industry || industry || 'general';

  const [rawBills, rawInvoices, rawAccounts, rawCustomers, rawBillPayments, reports] = await Promise.all([
    getBills(forceRefresh),
    getInvoices(forceRefresh),
    getAccounts(forceRefresh),
    getCustomers(forceRefresh),
    getBillPayments(forceRefresh),
    getReports(forceRefresh),
  ]);

  const bills = rawBills.map(normalizeBill);
  const invoices = rawInvoices.map(normalizeInvoice);

  const openBills = bills.filter((b) => b.balance > 0);
  const openInvoices = invoices.filter((i) => i.balance > 0);

  const payablesKpis = buildPayablesKpis(openBills);
  const receivablesKpis = buildReceivablesKpis(openInvoices);
  const liquidity = buildLiquidity(rawAccounts, payablesKpis);
  const financialHealth = buildFinancialHealth(reports);
  const paymentBehavior = buildPaymentBehavior(rawBills, rawBillPayments);
  const controls = buildControls(openBills, openInvoices, financialHealth);

  const payablesPriorities = buildPayablesPriorities(openBills, liquidity, financialHealth, paymentBehavior, controls);
  const collectionsPriorities = buildCollectionsPriorities(openInvoices, receivablesKpis);
  const readiness = buildReadiness(financialHealth, controls, liquidity, payablesKpis, receivablesKpis, intakeProfile);
  const alerts = buildAlerts(payablesKpis, receivablesKpis, liquidity, financialHealth, controls);
  const escalation = buildEscalation(readiness, controls, intakeProfile);

  const blueprint = {
    meta: {
      companyId: COMPANY_ID,
      generatedAt: new Date().toISOString(),
      industry: selectedIndustry,
      quickbooksRealmId: realmId,
    },
    intake: {
      available: Boolean(intakeProfile),
      schema: buildRemoteIntakeSchema(selectedIndustry),
      profile: intakeProfile ? {
        industry: intakeProfile.industry,
        payload: intakeProfile.payload,
        updatedAt: intakeProfile.updated_at,
      } : null,
    },
    payables: {
      ...payablesKpis,
      priorities: payablesPriorities.slice(0, 50),
    },
    receivables: {
      ...receivablesKpis,
      priorities: collectionsPriorities.slice(0, 50),
    },
    liquidity,
    financialHealth,
    controls: {
      duplicateBillCount: controls.duplicateBillCount,
      missingBillNoCount: controls.missingBillNoCount,
      missingDueDateCount: controls.missingDueDateCount,
      missingInvoiceNoCount: controls.missingInvoiceNoCount,
      negativeCurrentRatio: controls.negativeCurrentRatio,
    },
    paymentBehavior: paymentBehavior.metrics
      ? paymentBehavior
      : {
          available: paymentBehavior.available,
          metrics: null,
          vendorStats: {},
        },
    readiness,
    alerts,
    escalation,
    history: { points: [], trend: null },
    recommendedActions: {
      payNow: payablesPriorities.filter((x) => x.action === 'Pay Now').slice(0, 10),
      paySoon: payablesPriorities.filter((x) => x.action === 'Pay Soon').slice(0, 10),
      reviewNow: [
        ...payablesPriorities.filter((x) => x.action === 'Review').slice(0, 5).map((x) => ({ type: 'bill', ...x })),
        ...collectionsPriorities.filter((x) => x.action !== 'Monitor').slice(0, 5).map((x) => ({ type: 'invoice', ...x })),
      ].slice(0, 10),
    },
  };

  const overlay = applyIndustryOverlay(selectedIndustry, blueprint, intakeProfile);
  const finalPayload = {
    ...blueprint,
    overlay,
  };

  await saveSnapshotIfChanged(finalPayload);
  const historyRows = await getSnapshotHistory(12);
  finalPayload.history = buildHistorySummary(historyRows);

  lastSyncTime = new Date();
  syncStatus = 'Success';

  return finalPayload;
}

app.get('/', (req, res) => {
  const oauthUrl =
    `https://appcenter.intuit.com/connect/oauth2` +
    `?client_id=${encodeURIComponent(QB_CLIENT_ID)}` +
    `&response_type=code` +
    `&scope=${encodeURIComponent('com.intuit.quickbooks.accounting')}` +
    `&redirect_uri=${encodeURIComponent(QB_REDIRECT_URI)}` +
    `&state=${encodeURIComponent(QB_OAUTH_STATE)}`;

  res.send(`
    <html>
      <body style="font-family: Arial, sans-serif; padding: 30px;">
        <h1>Final QuickBooks Blueprint API</h1>
        <p>Universal SMB blueprint engine with remote intake and industry overlays.</p>
        <p><a href="${oauthUrl}">Connect QuickBooks</a></p>
        <ul>
          <li><a href="/api/health">/api/health</a></li>
          <li><a href="/api/templates">/api/templates</a></li>
          <li><a href="/api/intake/schema">/api/intake/schema</a></li>
          <li><a href="/api/final-blueprint">/api/final-blueprint</a></li>
          <li><a href="/api/readiness">/api/readiness</a></li>
          <li><a href="/api/priorities">/api/priorities</a></li>
          <li><a href="/api/history">/api/history</a></li>
        </ul>
      </body>
    </html>
  `);
});

app.get('/callback', async (req, res) => {
  try {
    const code = req.query.code;
    const state = req.query.state;
    const callbackRealmId = req.query.realmId;

    if (state !== QB_OAUTH_STATE) {
      return res.status(400).send('Invalid OAuth state.');
    }

    if (!code || !callbackRealmId) {
      return res.status(400).send('Missing code or realmId.');
    }

    const response = await axios.post(
      'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
      `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent(QB_REDIRECT_URI)}`,
      {
        headers: {
          Authorization: 'Basic ' + Buffer.from(`${QB_CLIENT_ID}:${QB_CLIENT_SECRET}`).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded',
          Accept: 'application/json',
        },
      }
    );

    realmId = callbackRealmId;
    accessToken = response.data.access_token;
    refreshToken = response.data.refresh_token || '';
    tokenExpiresAt = response.data.expires_in ? new Date(Date.now() + response.data.expires_in * 1000) : null;
    refreshExpiresAt = response.data.x_refresh_token_expires_in ? new Date(Date.now() + response.data.x_refresh_token_expires_in * 1000) : null;

    await saveConnection();

    res.send(`
      <html>
        <body style="font-family: Arial, sans-serif; padding: 30px;">
          <h1>Connected</h1>
          <p>QuickBooks is connected.</p>
          <p><a href="/api/final-blueprint">Open final blueprint</a></p>
        </body>
      </html>
    `);
  } catch (err) {
    res.status(500).send(`<pre>${JSON.stringify(err.response?.data || err.message, null, 2)}</pre>`);
  }
});

app.get('/api/health', async (req, res) => {
  try {
    const saved = await pool.query(
      `SELECT company_id, realm_id, updated_at
       FROM qb_connections
       WHERE company_id = $1
       LIMIT 1`,
      [COMPANY_ID]
    );

    res.json({
      ok: true,
      companyId: COMPANY_ID,
      quickbooksConnected: Boolean(accessToken && realmId),
      savedConnection: saved.rowCount > 0,
      realmId: realmId || saved.rows[0]?.realm_id || null,
      lastSyncTime,
      syncStatus,
      environment: QB_ENV,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/templates', (req, res) => {
  res.json({ ok: true, templates: INDUSTRY_TEMPLATES });
});

app.get('/api/intake/schema', async (req, res) => {
  const industry = req.query.industry || 'general';
  res.json({ ok: true, schema: buildRemoteIntakeSchema(industry) });
});

app.post('/api/intake', async (req, res) => {
  try {
    const industry = req.body.industry || 'general';
    const payload = req.body.payload || {};
    await saveIntakeProfile({ companyId: COMPANY_ID, industry, payload });
    res.json({ ok: true, saved: true, industry, payload });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/intake', async (req, res) => {
  try {
    const profile = await loadIntakeProfile(COMPANY_ID);
    res.json({ ok: true, profile });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/final-blueprint', async (req, res) => {
  try {
    const industry = req.query.industry || 'general';
    const payload = await buildFinalCoreBlueprint({
      forceRefresh: req.query.refresh === 'true',
      industry,
    });

    res.json({ ok: true, blueprint: payload });
  } catch (err) {
    syncStatus = 'Failed';
    res.status(500).json({ ok: false, error: err.response?.data || err.message });
  }
});

app.get('/api/readiness', async (req, res) => {
  try {
    const industry = req.query.industry || 'general';
    const payload = await buildFinalCoreBlueprint({
      forceRefresh: req.query.refresh === 'true',
      industry,
    });

    res.json({
      ok: true,
      readiness: payload.readiness,
      controls: payload.controls,
      escalation: payload.escalation,
      alerts: payload.alerts,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.response?.data || err.message });
  }
});

app.get('/api/priorities', async (req, res) => {
  try {
    const industry = req.query.industry || 'general';
    const payload = await buildFinalCoreBlueprint({
      forceRefresh: req.query.refresh === 'true',
      industry,
    });

    res.json({
      ok: true,
      payables: payload.payables.priorities,
      receivables: payload.receivables.priorities,
      recommendedActions: payload.recommendedActions,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.response?.data || err.message });
  }
});

app.get('/api/history', async (req, res) => {
  try {
    const rows = await getSnapshotHistory(Number(req.query.limit || 12));
    res.json({ ok: true, history: buildHistorySummary(rows) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

async function boot() {
  await initDB();
  await loadConnection().catch(() => null);

  app.listen(PORT, () => {
    console.log(`Final QuickBooks Blueprint API running on port ${PORT}`);
  });
}

boot().catch((err) => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
