// ================================
// FULL MERGED MULTI-TENANT VERSION
// (Based on your original large app.js, upgraded—not replaced)
// ================================

import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import fs from "fs";

dotenv.config();

const app = express();
app.use(express.json());

// ================================
// TENANT STORE (replaces global vars)
// ================================

const tenants = {};

function getTenant(companyId) {
  if (!tenants[companyId]) {
    tenants[companyId] = {
      accessToken: null,
      refreshToken: null,
      realmId: null,
      billsCache: [],
      dashboardHistory: []
    };
  }
  return tenants[companyId];
}

// ================================
// FIXED MEDIAN (your original had bug)
// ================================

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

// ================================
// QUICKBOOKS AUTH (NOW TENANT-AWARE)
// ================================

app.get("/auth/:companyId", (req, res) => {
  const { companyId } = req.params;

  const url = `https://appcenter.intuit.com/connect/oauth2?` +
    `client_id=${process.env.CLIENT_ID}` +
    `&redirect_uri=${process.env.REDIRECT_URI}` +
    `&response_type=code` +
    `&scope=com.intuit.quickbooks.accounting` +
    `&state=${companyId}`;

  res.redirect(url);
});

app.get("/callback", async (req, res) => {
  const { code, realmId, state } = req.query;
  const companyId = state;

  const tenant = getTenant(companyId);

  try {
    const tokenRes = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      new URLSearchParams({
        grant_type: "authorization_code",
        code,
        redirect_uri: process.env.REDIRECT_URI
      }),
      {
        headers: {
          Authorization:
            "Basic " +
            Buffer.from(
              `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET}`
            ).toString("base64"),
          "Content-Type": "application/x-www-form-urlencoded"
        }
      }
    );

    tenant.accessToken = tokenRes.data.access_token;
    tenant.refreshToken = tokenRes.data.refresh_token;
    tenant.realmId = realmId;

    res.send(`Connected company: ${companyId}`);
  } catch (err) {
    console.error(err.response?.data || err.message);
    res.status(500).send("OAuth failed");
  }
});

// ================================
// FETCH BILLS (TENANT SAFE)
// ================================

app.get("/fetch-bills/:companyId", async (req, res) => {
  const { companyId } = req.params;
  const tenant = getTenant(companyId);

  if (!tenant.accessToken) {
    return res.status(400).send("Not connected");
  }

  try {
    const query = encodeURIComponent("SELECT * FROM Bill MAXRESULTS 100");

    const response = await axios.get(
      `https://quickbooks.api.intuit.com/v3/company/${tenant.realmId}/query?query=${query}`,
      {
        headers: {
          Authorization: `Bearer ${tenant.accessToken}`,
          Accept: "application/json"
        }
      }
    );

    const bills = response.data.QueryResponse.Bill || [];

    tenant.billsCache = bills;

    res.json({ count: bills.length, companyId });
  } catch (err) {
    console.error(err.response?.data || err.message);
    res.status(500).send("Failed to fetch bills");
  }
});

// ================================
// ANALYTICS ENGINE (kept from yours, just cleaned)
// ================================

function analyzeBills(bills) {
  const amounts = bills.map(b => b.TotalAmt || 0);

  const avg = amounts.reduce((a, b) => a + b, 0) / (amounts.length || 1);
  const med = median(amounts);

  const anomalies = bills.filter(b => (b.TotalAmt || 0) > avg * 2);

  return {
    avgBill: avg,
    medianBill: med,
    anomalies: anomalies.length,
    totalBills: bills.length
  };
}

// ================================
// DASHBOARD (TENANT SAFE)
// ================================

app.get("/dashboard/:companyId", (req, res) => {
  const { companyId } = req.params;
  const tenant = getTenant(companyId);

  const analysis = analyzeBills(tenant.billsCache);

  const snapshot = {
    timestamp: Date.now(),
    ...analysis
  };

  tenant.dashboardHistory.push(snapshot);

  res.json({
    companyId,
    current: snapshot,
    history: tenant.dashboardHistory.slice(-10)
  });
});

// ================================
// SERVER
// ================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
