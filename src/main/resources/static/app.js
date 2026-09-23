const REFRESH_MS = 2000;
const builtFailureInjectionRows = new Set();

async function api(path, options) {
  const res = await fetch(path, options);
  if (!res.ok) {
    throw new Error(`${path} -> HTTP ${res.status}`);
  }
  return res.status === 204 ? null : res.json();
}

function fmtNum(n, decimals = 1) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  return Number(n).toFixed(decimals);
}

function fmtPercent(n) {
  return fmtNum(n, 1) + '%';
}

function fmtTime(iso) {
  if (!iso) return '-';
  const d = new Date(iso);
  return d.toLocaleTimeString();
}

function statusBadge(text) {
  const cls = 'badge-' + String(text).toLowerCase();
  return `<span class="badge ${cls}">${text}</span>`;
}

// ---------- System overview ----------

async function refreshSystemStats() {
  const m = await api('/api/metrics/system');
  const stats = [
    ['Ingestion / sec', fmtNum(m.ingestionPerSecond)],
    ['Processing / sec', fmtNum(m.processingPerSecond)],
    ['Delivery / sec', fmtNum(m.deliveryPerSecond)],
    ['Success Rate', fmtPercent(m.successRatePercent)],
    ['Retry Rate / sec', fmtNum(m.retryRatePerSecond)],
    ['DLQ Count', m.dlqTotalCount],
    ['Scheduled Queue Depth', m.scheduledQueueDepth],
    ['Consumer Lag', m.consumerLagTotal],
    ['Active C2 Workers', m.activeC2Workers],
    ['Total Processed', m.totalProcessed],
    ['Total Delivered', m.totalDelivered],
    ['Total Retries Scheduled', m.totalRetryScheduled],
  ];
  document.getElementById('system-stats').innerHTML = stats.map(([label, value]) => `
    <div class="stat">
      <div class="label">${label}</div>
      <div class="value">${value}</div>
    </div>
  `).join('');
}

// ---------- Traffic generator ----------

let merchantListCache = [];

async function loadMerchantCheckboxes() {
  merchantListCache = await api('/api/merchants');
  document.getElementById('tg-merchants').innerHTML = merchantListCache.map(id => `
    <label><input type="checkbox" value="${id}"> ${id}</label>
  `).join('');
}

async function refreshTrafficStatus() {
  const s = await api('/api/traffic/status');
  const el = document.getElementById('tg-status');
  if (s.running) {
    const stop = s.willStopAt ? new Date(s.willStopAt).toLocaleTimeString() : 'manual stop';
    el.textContent = `running: target ${s.targetEventsPerSecond}/s, actual ${fmtNum(s.actualEventsPerSecond)}/s, ` +
      `published ${s.totalPublished}, merchants [${s.merchantIds.join(', ')}], stops at ${stop}`;
  } else {
    el.textContent = `not running (total published so far: ${s.totalPublished})`;
  }
}

function setupTrafficControls() {
  document.getElementById('tg-start').addEventListener('click', async () => {
    const eventsPerSecond = parseInt(document.getElementById('tg-eps').value, 10);
    const durationRaw = document.getElementById('tg-duration').value;
    const durationSeconds = durationRaw ? parseInt(durationRaw, 10) : null;
    const merchantIds = Array.from(document.querySelectorAll('#tg-merchants input:checked')).map(el => el.value);

    await api('/api/traffic/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ eventsPerSecond, durationSeconds, merchantIds }),
    });
    refreshTrafficStatus();
  });

  document.getElementById('tg-stop').addEventListener('click', async () => {
    await api('/api/traffic/stop', { method: 'POST' });
    refreshTrafficStatus();
  });
}

// ---------- Merchant health ----------

async function refreshMerchantHealth() {
  const list = await api('/api/merchants/health');
  document.getElementById('merchant-health-body').innerHTML = list.map(m => `
    <tr>
      <td>${m.merchantId}</td>
      <td>${statusBadge(m.status)}</td>
      <td>${fmtNum(m.requestRatePerSecond)}</td>
      <td>${fmtPercent(m.successRatePercent)}</td>
      <td>${fmtPercent(m.failureRatePercent)}</td>
      <td>${fmtNum(m.averageLatencyMs)}</td>
      <td>${statusBadge(m.circuitBreakerState)}</td>
      <td>${m.activeRequests} / ${m.concurrencyLimit}</td>
    </tr>
  `).join('');
  renderFailureInjectionRows(list);
}

// ---------- Failure injection ----------

const FAILURE_MODES = ['SUCCESS', 'HTTP_4XX', 'HTTP_429', 'HTTP_500', 'HTTP_503', 'TIMEOUT'];

function renderFailureInjectionRows(merchantHealthList) {
  const tbody = document.getElementById('failure-injection-body');
  for (const m of merchantHealthList) {
    if (builtFailureInjectionRows.has(m.merchantId)) continue;
    builtFailureInjectionRows.add(m.merchantId);

    const fi = m.failureInjection || { mode: 'SUCCESS', failurePercentage: 0, latencyMs: 0, specific4xxStatus: 400 };
    const row = document.createElement('tr');
    row.className = 'fi-row';
    row.innerHTML = `
      <td>${m.merchantId}</td>
      <td><select class="fi-mode">${FAILURE_MODES.map(mode =>
        `<option value="${mode}" ${mode === fi.mode ? 'selected' : ''}>${mode}</option>`).join('')}</select></td>
      <td><input type="number" class="fi-pct" min="0" max="100" value="${fi.failurePercentage}" style="width:60px"></td>
      <td><input type="number" class="fi-latency" min="0" value="${fi.latencyMs}" style="width:70px"></td>
      <td><input type="number" class="fi-4xx" min="400" max="499" value="${fi.specific4xxStatus}" style="width:60px"></td>
      <td><button class="small">Apply</button></td>
    `;
    row.querySelector('button').addEventListener('click', async () => {
      const body = {
        mode: row.querySelector('.fi-mode').value,
        failurePercentage: parseInt(row.querySelector('.fi-pct').value, 10) || 0,
        latencyMs: parseInt(row.querySelector('.fi-latency').value, 10) || 0,
        specific4xxStatus: parseInt(row.querySelector('.fi-4xx').value, 10) || 400,
      };
      await api(`/api/merchants/${encodeURIComponent(m.merchantId)}/failure-injection`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
    });
    tbody.appendChild(row);
  }
}

// ---------- Retry queue ----------

async function refreshRetryQueue() {
  const list = await api('/api/retry-queue');
  const shown = list.slice(0, 100);
  document.getElementById('retry-queue-summary').textContent =
    list.length > shown.length ? `Showing ${shown.length} of ${list.length} pending` : `${list.length} pending`;
  document.getElementById('retry-queue-body').innerHTML = shown.map(r => `
    <tr>
      <td>${r.paymentId}</td>
      <td>${r.merchantId.value}</td>
      <td>${r.attempt}</td>
      <td>${fmtTime(r.nextAttemptAt)}</td>
      <td>${r.lastFailureDetail || ''}</td>
    </tr>
  `).join('');
}

// ---------- DLQ ----------

async function refreshDlq() {
  const list = await api('/api/dlq');
  const shown = list.slice(-100).reverse();
  document.getElementById('dlq-summary').textContent =
    list.length > shown.length ? `Showing latest ${shown.length} of ${list.length}` : `${list.length} entries`;
  document.getElementById('dlq-body').innerHTML = shown.map(d => `
    <tr>
      <td>${d.eventId}</td>
      <td>${d.paymentId}</td>
      <td>${d.merchantId.value}</td>
      <td>${d.attemptsMade}</td>
      <td>${d.failureReason}</td>
      <td>${fmtTime(d.movedToDlqAt)}</td>
    </tr>
  `).join('');
}

// ---------- Recent notification history ----------

async function refreshHistory() {
  const list = await api('/api/metrics/history');
  const shown = list.slice(-30).reverse();
  document.getElementById('history-body').innerHTML = shown.map(h => `
    <tr>
      <td>${fmtTime(h.timestamp)}</td>
      <td>${h.source}</td>
      <td>${h.paymentId}</td>
      <td>${h.merchantId.value}</td>
      <td>${h.attempt}</td>
      <td>${h.success ? statusBadge('HEALTHY') : (h.httpStatus || h.failureDetail || 'failed')}</td>
      <td>${fmtNum(h.latencyMs, 0)}</td>
    </tr>
  `).join('');
}

// ---------- Main loop ----------

async function refreshAll() {
  const tasks = [refreshSystemStats(), refreshTrafficStatus(), refreshMerchantHealth(),
    refreshRetryQueue(), refreshDlq(), refreshHistory()];
  const results = await Promise.allSettled(tasks);
  results.forEach(r => { if (r.status === 'rejected') console.error(r.reason); });
}

(async function init() {
  await loadMerchantCheckboxes();
  setupTrafficControls();
  await refreshAll();
  setInterval(refreshAll, REFRESH_MS);
})();
