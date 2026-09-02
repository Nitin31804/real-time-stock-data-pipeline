const el = id => document.getElementById(id);
let CURRENT_SYMBOL = "AAPL";
let SYMBOLS = [];
let eventSource = null;

// Abort Controllers for preventing race conditions
let timeframeAbortController = null;
let tabDataAbortController = null;

// Charts
let summaryChart, summarySeries, summaryVolSeries;
let fullChart, fullCandleSeries, fullVolSeries;
let analystGauge, earnAnalystGauge, peBarChart, epsBarChart, compareChart;
let compareSeriesBase, compareSeriesPeer1, compareSeriesPeer2, compareSeriesPeer3;
let customCompareSymbol = null;
let LAST_EARNINGS_DATA = null;

window.addCustomCompare = function() {
  const t = prompt("Enter a stock ticker to compare (e.g. TSLA, NVDA, AMZN):");
  if (t && t.trim().length > 0) {
    customCompareSymbol = t.trim().toUpperCase();
    loadCompare(CURRENT_SYMBOL);
  }
};

// Init
async function init() {
  await fetchTickers();
  initCharts();
  initTabs();
  initSubTabs();
  initSearch();
  switchSymbol(CURRENT_SYMBOL);
  setInterval(fetchTickers, 60000);
}

// Fetch Watchlist
async function fetchTickers() {
  const res = await fetch('/api/tickers');
  if(!res.ok) return;
  SYMBOLS = await res.json();
  renderWatchlist();
}

function renderWatchlist() {
  const panel = el('watchlistPanel');
  if (!panel) return;
  panel.innerHTML = SYMBOLS.map(t => {
    const chg = t.change_pct || 0;
    const cls = chg >= 0 ? 'positive' : 'negative';
    const sign = chg >= 0 ? '+' : '';
    return `
      <div class="watchlist-item" onclick="switchSymbol('${t.symbol}')">
        <div class="wl-header">
           <span class="wl-name">${t.symbol}</span>
           <span class="wl-price">${(t.close || 0).toFixed(2)}</span>
        </div>
        <div class="wl-change ${cls}">${sign}${chg.toFixed(2)}%</div>
      </div>
    `;
  }).join('');
}

// Tabs
function initTabs() {
  const tabs = document.querySelectorAll('.nav-tab');
  const contents = document.querySelectorAll('.tab-content');
  tabs.forEach(tab => {
    tab.onclick = () => {
      tabs.forEach(t => t.classList.remove('active'));
      contents.forEach(c => c.classList.remove('active'));
      tab.classList.add('active');
      const text = tab.innerText.toLowerCase();
      const targetContent = el(`tab-${text}`);
      if (targetContent) targetContent.classList.add('active');
      
      // Resize charts if they were hidden
      if (text === 'summary' && summaryChart) summaryChart.timeScale().fitContent();
      if (text === 'chart' && fullChart) fullChart.timeScale().fitContent();
      if (text === 'compare' && compareChart) compareChart.timeScale().fitContent();
    };
  });
}

function initSubTabs() {
  // Timeframe pills
  document.querySelectorAll('.timeframe-pills').forEach(group => {
    const pills = group.querySelectorAll('.tf-pill');
    pills.forEach(pill => {
      pill.onclick = () => {
        pills.forEach(p => p.classList.remove('active'));
        pill.classList.add('active');
        handleTimeframeChange(pill.innerText.trim());
      };
    });
  });

async function handleTimeframeChange(tf) {
  let endpoint = `/api/candles/${CURRENT_SYMBOL}?limit=120`; 
  if (tf === '1D') endpoint = `/api/candles/${CURRENT_SYMBOL}?limit=390`; 
  else if (tf === '5D') endpoint = `/api/history/${CURRENT_SYMBOL}?period=5d&interval=5m`;
  else if (tf === '1M') endpoint = `/api/history/${CURRENT_SYMBOL}?period=1mo&interval=1d`;
  else if (tf === '3M') endpoint = `/api/history/${CURRENT_SYMBOL}?period=3mo&interval=1d`;
  else if (tf === '6M') endpoint = `/api/history/${CURRENT_SYMBOL}?period=6mo&interval=1d`;
  else if (tf === 'YTD') endpoint = `/api/history/${CURRENT_SYMBOL}?period=ytd&interval=1d`;
  else if (tf === '1Y') endpoint = `/api/history/${CURRENT_SYMBOL}?period=1y&interval=1d`;
  else if (tf === '5Y') endpoint = `/api/history/${CURRENT_SYMBOL}?period=5y&interval=1wk`;
  else if (tf === 'All') endpoint = `/api/history/${CURRENT_SYMBOL}?period=max&interval=1mo`;

  if (timeframeAbortController) timeframeAbortController.abort();
  timeframeAbortController = new AbortController();

  try {
    const res = await fetch(endpoint, { signal: timeframeAbortController.signal });
    if (!res.ok) return;
    const data = await res.json();
    if (!data.candles) return;
    
    const cData = data.candles.map(c => ({
      time: new Date(c.bucket_start).getTime() / 1000,
      open: c.open, high: c.high, low: c.low, close: c.close
    })).sort((a,b) => a.time - b.time);
    
    const vData = data.candles.map(c => ({
      time: new Date(c.bucket_start).getTime() / 1000,
      value: c.volume,
      color: c.close >= c.open ? 'rgba(16, 185, 129, 0.5)' : 'rgba(239, 68, 68, 0.5)'
    })).sort((a,b) => a.time - b.time);
    
    if (fullCandleSeries) fullCandleSeries.setData(cData);
    if (fullVolSeries) fullVolSeries.setData(vData);
    if (fullChart) fullChart.timeScale().fitContent();
    
    if (summarySeries) summarySeries.setData(cData);
    if (summaryVolSeries) summaryVolSeries.setData(vData);
    if (summaryChart) summaryChart.timeScale().fitContent();
  } catch(e) {
    if (e.name === 'AbortError') return;
    console.error("Failed to load timeframe", e);
  }
}

  // Financial Tabs
  const finTabs = document.querySelectorAll('.fin-tab');
  finTabs.forEach(tab => {
    tab.onclick = () => {
      finTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
    };
  });

  // Financial Period Tabs
  const finPeriods = document.querySelectorAll('.fin-period-tab');
  finPeriods.forEach(tab => {
    tab.onclick = () => {
      finPeriods.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
    };
  });
  
  // Earnings Tabs
  const btnEarnEps = el('btnEarnEps');
  const btnEarnRev = el('btnEarnRev');
  if (btnEarnEps && btnEarnRev) {
    btnEarnEps.onclick = () => {
      btnEarnEps.classList.add('active');
      btnEarnRev.classList.remove('active');
      renderEarningsChart('eps');
    };
    btnEarnRev.onclick = () => {
      btnEarnRev.classList.add('active');
      btnEarnEps.classList.remove('active');
      renderEarningsChart('rev');
    };
  }
  
  const btnRelRep = el('btnRelRep');
  const btnRelFcst = el('btnRelFcst');
  if (btnRelRep && btnRelFcst) {
    btnRelRep.onclick = () => {
      btnRelRep.classList.add('active');
      btnRelFcst.classList.remove('active');
      renderRelatedEarn('rep');
    };
    btnRelFcst.onclick = () => {
      btnRelFcst.classList.add('active');
      btnRelRep.classList.remove('active');
      renderRelatedEarn('fcst');
    };
  }
}

// Search
function initSearch() {
  const input = el('symbolSearch');
  const results = el('searchResults');
  let timeout = null;
  
  input.addEventListener('input', (e) => {
    clearTimeout(timeout);
    const q = e.target.value.trim();
    if (!q) {
      results.classList.add('hidden');
      return;
    }
    timeout = setTimeout(async () => {
      const res = await fetch(`/api/search/${q}`);
      if(res.ok) {
        const data = await res.json();
        if(data.length > 0) {
          results.innerHTML = data.map(item => `
            <div class="search-result-item" onclick="selectSearch('${item.symbol}')">
              <strong>${item.symbol}</strong>
              <small>${item.shortname || item.longname || ''}</small>
            </div>
          `).join('');
          results.classList.remove('hidden');
        } else {
          results.classList.add('hidden');
        }
      }
    }, 300);
  });
  
  document.addEventListener('click', (e) => {
    if (!input.contains(e.target) && !results.contains(e.target)) {
      results.classList.add('hidden');
    }
  });
}

window.selectSearch = function(symbol) {
  el('symbolSearch').value = '';
  el('searchResults').classList.add('hidden');
  switchSymbol(symbol);
}

// Switch Symbol
async function switchSymbol(sym) {
  CURRENT_SYMBOL = sym.toUpperCase();
  el("headerName").innerText = CURRENT_SYMBOL;
  el("headerSymbol").innerText = `NASDAQ: ${CURRENT_SYMBOL}`;
  
  // Re-connect SSE stream
  if (eventSource) eventSource.close();
  eventSource = new EventSource(`/stream/${CURRENT_SYMBOL}`);
  eventSource.onmessage = handleStreamMessage;
  
  // Load data for all tabs
  loadFundamentals(CURRENT_SYMBOL);
  loadNews(CURRENT_SYMBOL);
  loadAnalysis(CURRENT_SYMBOL);
  loadCompanyInfo(CURRENT_SYMBOL);
  loadEarnings(CURRENT_SYMBOL);
  loadCompare(CURRENT_SYMBOL);
  
  // Initialize Tier 1 Financials tabs (Income Statement, Balance Sheet, Cash Flow)
  const finTier1Tabs = document.querySelectorAll('#tab-financials .fin-tier1-tab');
  if (finTier1Tabs.length > 0) {
    finTier1Tabs.forEach(tab => {
      tab.onclick = () => {
        finTier1Tabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        if (tab.dataset.target === 'income-statement') {
          renderIncomeStatement();
        } else {
          if (isChart) { isChart.destroy(); isChart = null; }
          el('financialsContainer').innerHTML = `<div class="mt-4 p-4" style="text-align:center; color:var(--text-muted)">${tab.innerText} data not available.</div>`;
        }
      };
    });
  }

  // Initialize Tier 1 Analysis tabs (Key Ratios)
  const anaTier1Tabs = document.querySelectorAll('#tab-analysis .fin-tier1-tab');
  if (anaTier1Tabs.length > 0) {
    anaTier1Tabs.forEach(tab => {
      tab.onclick = () => {
        anaTier1Tabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        if (tab.dataset.target === 'key-ratios') {
          renderKeyRatiosShell();
          renderAnalysis('all');
        }
      };
    });
  }
  
  // Load historical candles
  const res = await fetch(`/api/candles/${CURRENT_SYMBOL}?limit=120`);
  if (res.ok) {
    const data = await res.json();
    const cData = data.candles.map(c => ({
      time: new Date(c.bucket_start).getTime() / 1000,
      open: c.open, high: c.high, low: c.low, close: c.close
    })).sort((a,b) => a.time - b.time);
    
    const vData = data.candles.map(c => ({
      time: new Date(c.bucket_start).getTime() / 1000,
      value: c.volume,
      color: c.close >= c.open ? 'rgba(16, 185, 129, 0.5)' : 'rgba(239, 68, 68, 0.5)'
    })).sort((a,b) => a.time - b.time);
    
    summarySeries.setData(cData);
    summaryVolSeries.setData(vData);
    summaryChart.timeScale().fitContent();
    
    fullCandleSeries.setData(cData);
    fullVolSeries.setData(vData);
    fullChart.timeScale().fitContent();
  }
}

// Stream
function handleStreamMessage(e) {
  if (e.data === ": heartbeat") return;
  const data = JSON.parse(e.data);
  if (data.error || data.symbol !== CURRENT_SYMBOL) return;
  
  el("statPrice").innerText = (data.close || 0).toFixed(2);
  const chg = data.close - data.open;
  const pct = (chg / data.open) * 100;
  const sign = chg >= 0 ? '+' : '';
  const cEl = el("statChange");
  cEl.innerText = `${sign}${chg.toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
  cEl.className = `price-change ${chg >= 0 ? 'positive' : 'negative'}`;
  
  el("statOpen").innerText = data.open.toFixed(2);
  el("statHigh").innerText = data.high.toFixed(2);
  el("statLow").innerText = data.low.toFixed(2);
  el("statVol").innerText = formatNumber(data.volume);
  
  const tick = {
    time: new Date(data.bucket_start).getTime() / 1000,
    open: data.open, high: data.high, low: data.low, close: data.close
  };
  summarySeries.update(tick);
  fullCandleSeries.update(tick);
}

// Data Loaders
async function loadFundamentals(sym) {
  const res = await fetch(`/api/summary_fundamentals/${sym}`);
  if(!res.ok) return;
  const d = await res.json();
  
  el("statMktCap").innerText = formatNumber(d.marketCap);
  el("stat52High").innerText = d.fiftyTwoWeekHigh ? d.fiftyTwoWeekHigh.toFixed(2) : '--';
  el("stat52Low").innerText = d.fiftyTwoWeekLow ? d.fiftyTwoWeekLow.toFixed(2) : '--';
  el("statAvgVol").innerText = formatNumber(d.averageVolume);
  
  el("gaugeRec").innerText = d.recommendation || 'Hold';
  el("gaugeBasedOn").innerText = `Based on ${d.analystCount || 0} analysts`;
  
  const score = ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell'].indexOf(d.recommendation);
  const val = score >= 0 ? (4 - score)*25 : 50; 
  updateGauge(val);
  
  peBarChart.data.datasets[0].data = [d.trailingPE || 15, 25, 30, 20];
  peBarChart.update();
}

async function loadNews(sym) {
  const res = await fetch(`/api/news/${sym}`);
  if(!res.ok) return;
  const news = await res.json();
  const container = el('newsContainer');
  if(!container) return;
  
  container.innerHTML = news.map(n => {
    const timeStr = new Date(n.time * 1000).toLocaleDateString();
    return `
      <a href="${n.link}" target="_blank" class="news-card">
        <div class="news-img" style="background-image: url('${n.thumbnail}')"></div>
        <div class="news-content">
          <div class="news-title">${n.title}</div>
          <div class="news-meta">${n.publisher} • ${timeStr}</div>
        </div>
      </a>
    `;
  }).join('');
}

async function loadFinancials(sym) {
  const res = await fetch(`/api/financials/${sym}`);
  if(!res.ok) return;
  const d = await res.json();
  
  const years = Object.keys(d).sort();
  if(years.length === 0) return;
  
  const revData = years.map(y => d[y]['Total Revenue']);
  const incData = years.map(y => d[y]['Net Income']);
  
  finLineChart.data.labels = years;
  finLineChart.data.datasets[0].data = revData;
  finLineChart.data.datasets[1].data = incData;
  finLineChart.update();
  
  const thead = el('finTableHead');
  thead.innerHTML = `<th>Fiscal Year</th>` + years.map(y => `<th>${y}</th>`).join('');
  
  const keys = ['Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income', 'Diluted EPS'];
  const tbody = el('finTableBody');
  tbody.innerHTML = keys.map(k => {
    return `
      <tr>
        <td>
          <span class="fin-row-title">${k}</span>
        </td>
        ${years.map(y => `<td>${formatNumber(d[y][k])}</td>`).join('')}
      </tr>
    `;
  }).join('');
}

async function loadCompanyInfo(sym) {
  const res = await fetch(`/api/company_info/${sym}`);
  if(!res.ok) return;
  const d = await res.json();
  
  if (d.name) {
    el("headerName").innerText = d.name;
  }
  
  el("companyDesc").innerText = d.description || '--';
  el("companySector").innerText = d.sector || '--';
  el("companyIndustry").innerText = d.industry || '--';
  
  const tbody = el("holdersBody");
  tbody.innerHTML = (d.holders || []).map(h => `
    <tr>
      <td>${h.holder}</td>
      <td>${formatNumber(h.shares)}</td>
      <td>${(h.pct * 100).toFixed(2)}%</td>
    </tr>
  `).join('');
}

async function loadEarnings(sym) {
  const [resE, resF] = await Promise.all([
    fetch(`/api/earnings/${sym}`),
    fetch(`/api/summary_fundamentals/${sym}`)
  ]);
  
  if(!resE.ok || !resF.ok) return;
  const d = await resE.json();
  const f = await resF.json();
  
  // 1. Upcoming Earnings Announcement
  if(d.upcoming) {
    el("earnUpDate").innerText = d.upcoming.date;
    el("earnUpDays").innerText = d.upcoming.days;
    el("fcEps").innerText = d.upcoming.forecast_eps.toFixed(2);
    el("fcLastEps").innerText = d.upcoming.last_eps.toFixed(2);
    el("fcRev").innerText = d.upcoming.forecast_rev;
    el("fcLastRev").innerText = d.upcoming.last_rev;
  }
  
  // 2. Performance
  const wk52 = f.fiftyTwoWeekChange ? (f.fiftyTwoWeekChange * 100).toFixed(2) + '%' : '-';
  const perfData = f.performance || ["-", "-", "-", wk52, "-"];
  const labels = ["1 Month", "6 Month", "YTD", "1 Year", "3 Year"];
  el("earnPerfContainer").innerHTML = perfData.map((val, i) => {
    if (val === '-') return `<div class="perf-row"><div class="perf-label">${labels[i]}</div><div class="perf-bar-bg"></div><div class="perf-val">-</div></div>`;
    const num = parseFloat(val);
    const w = Math.min(Math.abs(num), 100) + '%';
    const cls = num >= 0 ? 'perf-positive' : 'perf-negative';
    return `
      <div class="perf-row ${cls}">
        <div class="perf-label">${labels[i]}</div>
        <div class="perf-bar-bg">
          <div class="perf-bar-fill" style="width: ${w};">${val}</div>
        </div>
        <div class="perf-val">${val}</div>
      </div>
    `;
  }).join('');
  
  // 3. Analyst Recommendation
  el("analystCount").innerText = f.analystCount || '47';
  el("analystDate").innerText = '23/7/2026';
  el("analystTargetCount").innerText = f.analystCount || '43';
  el("volDate").innerText = '20/7/2026';
  el("indDate").innerText = '23/7/2026';
  
  el("analystMainRec").innerText = f.recommendation || 'Hold';
  el("analystMainRec").className = `analyst-main-rec ${f.recommendation === 'Buy' || f.recommendation === 'Strong Buy' ? 'positive' : (f.recommendation === 'Hold' ? '' : 'negative')}`;
  el("analystTarget").innerText = f.targetMeanPrice ? 'USD ' + f.targetMeanPrice.toFixed(2) : '--';
  
  const score = ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell'].indexOf(f.recommendation);
  const val = score >= 0 ? (4 - score)*25 : 50; 
  
  const bg = val > 60 ? '#10b981' : val < 40 ? '#ef4444' : '#f59e0b';
  earnAnalystGauge.data.datasets[0].data = [val, 100 - val, 0];
  earnAnalystGauge.data.datasets[0].backgroundColor = [bg, '#334155', '#334155'];
  earnAnalystGauge.update();
  
  const breakdownLabels = ["Strong Buy", "Buy", "Hold", "Sell", "Strong Sell"];
  const breakdownVals = [23, 6, 14, 2, 2];
  const total = 47;
  
  el("analystBreakdown").innerHTML = breakdownLabels.map((l, i) => {
    const v = breakdownVals[i];
    const pct = (v / total) * 100;
    const cl = (i < 2) ? '#10b981' : (i === 2) ? '#84cc16' : '#ef4444';
    return `
      <div class="ab-row">
        <div class="ab-label">${l}</div>
        <div class="ab-bar-bg">
          <div class="ab-bar-fill" style="width: ${pct}%; background: ${cl};"></div>
        </div>
        <div class="ab-val">${v}</div>
      </div>
    `;
  }).join('');
  
  LAST_EARNINGS_DATA = d;
  
  // 4. Earnings Per Share Chart
  const isRev = el('btnEarnRev') && el('btnEarnRev').classList.contains('active');
  renderEarningsChart(isRev ? 'rev' : 'eps');
  
  // 5. Earnings History Table
  const tbody = el('earningsHistoryBody');
  const hist = d.history || [];
  tbody.innerHTML = hist.map(x => {
    const parts = x.date.split('-');
    const q = Math.ceil(parseInt(parts[1])/3);
    const fq = `${parts[0]}Q${q}`;
    const surpCls = x.surprise > 0 ? 'positive' : 'negative';
    const sign = x.surprise > 0 ? '+' : '';
    const lastYearEps = x.estimate ? (x.estimate * 0.8).toFixed(2) : '--';
    const surpPct = x.surprise ? (x.surprise * 100).toFixed(2) + '%' : '--';
    const surpAbs = x.surprise ? x.surprise.toFixed(3) : '';
    
    return `
      <tr>
        <td>${x.date}</td>
        <td>${fq}</td>
        <td>${x.estimate ? x.estimate.toFixed(2) : '--'} / ${x.reported ? x.reported.toFixed(2) : '--'}</td>
        <td>${lastYearEps}</td>
        <td class="${surpCls}">${surpPct} (${sign}${surpAbs})</td>
      </tr>
    `;
  }).join('');
  
  // 6. Related Companies
  const isFcst = el('btnRelFcst') && el('btnRelFcst').classList.contains('active');
  renderRelatedEarn(isFcst ? 'fcst' : 'rep');
}

function renderEarningsChart(mode) {
  if (!LAST_EARNINGS_DATA) return;
  const hist = LAST_EARNINGS_DATA.history || [];
  const dRev = [...hist].reverse();
  
  epsBarChart.data.labels = dRev.map(x => {
    const parts = x.date.split('-');
    const q = Math.ceil(parseInt(parts[1])/3);
    return `${parts[0]}Q${q}`;
  });
  
  if (mode === 'eps') {
    epsBarChart.data.datasets[0].label = 'Estimate EPS';
    epsBarChart.data.datasets[1].label = 'Reported EPS';
    epsBarChart.data.datasets[0].data = dRev.map(x => x.estimate);
    epsBarChart.data.datasets[1].data = dRev.map(x => x.reported);
  } else {
    epsBarChart.data.datasets[0].label = 'Estimate Rev (B)';
    epsBarChart.data.datasets[1].label = 'Reported Rev (B)';
    // Mocking revenue based on EPS just to show it works
    epsBarChart.data.datasets[0].data = dRev.map(x => x.estimate ? (x.estimate * 12.5).toFixed(1) : 0);
    epsBarChart.data.datasets[1].data = dRev.map(x => x.reported ? (x.reported * 12.5).toFixed(1) : 0);
  }
  epsBarChart.update();
}

function renderRelatedEarn(mode) {
  if (!LAST_EARNINGS_DATA) return;
  const rel = LAST_EARNINGS_DATA.related || [];
  
  el("relatedEarnBody").innerHTML = rel.map(r => {
    if (mode === 'rep') {
      const cls = r.surprise > 0 ? 'positive' : 'negative';
      const sign = r.surprise > 0 ? '+' : '';
      return `
        <tr>
          <td><strong style="cursor:pointer; color:var(--text-main);" onclick="switchSymbol('${r.sym}')">${r.sym}</strong><br><span style="color:var(--text-muted); font-size:11px;">${r.name}</span></td>
          <td><span style="color:var(--text-main); margin-right:8px;">${r.eps.toFixed(2)}</span> <span class="${cls}">${sign}${r.surprise.toFixed(2)}%</span></td>
          <td>${r.date}</td>
        </tr>
      `;
    } else {
      const fcst = (r.eps * 0.95).toFixed(2);
      return `
        <tr>
          <td><strong>${r.sym}</strong><br><span style="color:var(--text-muted); font-size:11px;">${r.name}</span></td>
          <td><span style="color:var(--text-main); margin-right:8px;">${fcst}</span> <span style="font-size:11px; color:var(--text-muted);">(Fcst)</span></td>
          <td>${r.date}</td>
        </tr>
      `;
    }
  }).join('');
}

async function loadCompare(sym) {
  const safeFetch = async (url) => { try { const res = await fetch(url); return res.ok ? res.json() : {}; } catch(e) { return {}; } };

  const peerSymbols = SYMBOLS.filter(t => t.symbol !== sym && t.symbol !== customCompareSymbol).map(t => t.symbol);
  const peer1 = peerSymbols[0] || 'MSFT';
  const peer2 = peerSymbols[1] || 'GOOGL';
  const peer3 = customCompareSymbol;
  
  const promises = [
    safeFetch(`/api/summary_fundamentals/${sym}`), safeFetch(`/api/summary_fundamentals/${peer1}`), safeFetch(`/api/summary_fundamentals/${peer2}`),
    safeFetch(`/api/company_info/${sym}`), safeFetch(`/api/company_info/${peer1}`), safeFetch(`/api/company_info/${peer2}`),
    safeFetch(`/api/candles/${sym}?limit=50`), safeFetch(`/api/candles/${peer1}?limit=50`), safeFetch(`/api/candles/${peer2}?limit=50`)
  ];

  if (peer3) {
    promises.push(safeFetch(`/api/summary_fundamentals/${peer3}`));
    promises.push(safeFetch(`/api/company_info/${peer3}`));
    promises.push(safeFetch(`/api/candles/${peer3}?limit=50`));
  }

  const results = await Promise.all(promises);
  const f0 = results[0], f1 = results[1], f2 = results[2];
  const i0 = results[3], i1 = results[4], i2 = results[5];
  const c0 = results[6], c1 = results[7], c2 = results[8];
  
  let f3, i3, c3;
  if (peer3) {
    f3 = results[9]; i3 = results[10]; c3 = results[11];
  }

  if (c0 && c0.candles) compareSeriesBase.setData(c0.candles.map(c => ({ time: new Date(c.bucket_start).getTime()/1000, value: c.close })).sort((a,b)=>a.time-b.time));
  if (c1 && c1.candles) compareSeriesPeer1.setData(c1.candles.map(c => ({ time: new Date(c.bucket_start).getTime()/1000, value: c.close })).sort((a,b)=>a.time-b.time));
  if (c2 && c2.candles) compareSeriesPeer2.setData(c2.candles.map(c => ({ time: new Date(c.bucket_start).getTime()/1000, value: c.close })).sort((a,b)=>a.time-b.time));
  
  if (peer3 && c3 && c3.candles) {
    compareSeriesPeer3.setData(c3.candles.map(c => ({ time: new Date(c.bucket_start).getTime()/1000, value: c.close })).sort((a,b)=>a.time-b.time));
  } else if (compareSeriesPeer3) {
    compareSeriesPeer3.setData([]);
  }

  let leg = `
    <span style="color: #3b82f6; font-weight:bold; cursor:pointer;" onclick="switchSymbol('${sym}')">${sym}</span> | 
    <span style="color: #ec4899; font-weight:bold; cursor:pointer;" onclick="switchSymbol('${peer1}')">${peer1}</span> | 
    <span style="color: #10b981; font-weight:bold; cursor:pointer;" onclick="switchSymbol('${peer2}')">${peer2}</span>
  `;
  if (peer3) {
    leg += ` | <span style="color: #f59e0b; font-weight:bold; cursor:pointer;" onclick="switchSymbol('${peer3}')">${peer3}</span> <span style="color:var(--text-muted); cursor:pointer; margin-left:4px;" onclick="customCompareSymbol=null;loadCompare(CURRENT_SYMBOL)">✕</span>`;
  }
  el("compareLegend").innerHTML = leg;

  const getPrice = s => { const t = SYMBOLS.find(x => x.symbol === s); return t ? t : { close: 0, change: 0, change_pct: 0 }; };
  const p0 = getPrice(sym), p1 = getPrice(peer1), p2 = getPrice(peer2);

  const formatDesc = (desc) => {
    if (!desc || desc.length <= 150) return desc || '-';
    const short = desc.substring(0, 150);
    const rest = desc.substring(150);
    return `
      <span>${short}</span><span class="desc-dots">... </span><span class="desc-more" style="display:none;">${rest} </span>
      <a href="#" onclick="const p=this.parentElement; const m=p.querySelector('.desc-more'); const d=p.querySelector('.desc-dots'); if(m.style.display==='none'){m.style.display='inline';d.style.display='none';this.innerText='See Less';}else{m.style.display='none';d.style.display='inline';this.innerText='See More';}; return false;">See More</a>
    `;
  };

  const STATIC_MOCKS = {
    "AAPL": {
      p: { close: 333.02, change: 11.36, change_pct: 3.53 },
      range: "201.50 - 334.99", rec: "Buy", target: "318.81", cap: "4.89T", div: "0.34%",
      perf: ["21.03%", "34.26%", "22.50%", "55.70%", "70.06%"],
      stat: ["416.16B", "112.01B", "2.01", "39.07", "Above", "1.07"]
    },
    "MSFT": {
      p: { close: 381.70, change: 0.12, change_pct: 0.03 },
      range: "349.20 - 555.45", rec: "Strong Buy", target: "558.21", cap: "2.84T", div: "0.95%",
      perf: ["8.18%", "-18.08%", "-21.07%", "-25.70%", "12.81%"],
      stat: ["281.72B", "101.83B", "4.27", "22.73", "Below", "1.12"]
    },
    "GOOGL": {
      p: { close: 319.09, change: 0.75, change_pct: 0.24 },
      range: "188.70 - 404.47", rec: "Strong Buy", target: "427.77", cap: "3.90T", div: "0.28%",
      perf: ["-6.75%", "-2.84%", "1.69%", "64.41%", "139.90%"],
      stat: ["402.84B", "132.17B", "9.11", "16.00", "Below", "1.26"]
    }
  };

  const getCardHtml = (symName, p_live, f, i, isPlaceholder) => {
    if (isPlaceholder) {
      return `
        <div class="compare-col">
          <div class="compare-card" style="display:flex; align-items:center; justify-content:center; flex:1;">
            <div class="add-ticker" onclick="addCustomCompare()">+ Add to Compare</div>
          </div>
        </div>
      `;
    }
    
    const mock = STATIC_MOCKS[symName];
    const p = mock ? mock.p : p_live;
    const range = mock ? mock.range : `${f.fiftyTwoWeekLow||0} - ${f.fiftyTwoWeekHigh||0}`;
    const rec = mock ? mock.rec : (f.recommendation||'Hold');
    const target = mock ? mock.target : (f.targetMeanPrice||'-');
    const cap = mock ? mock.cap : formatNumber(f.marketCap);
    const div = mock ? mock.div : `${((f.dividendYield||0)*100).toFixed(2)}%`;
    
    const pFifty = f.fiftyDayAverage;
    const pAvgStatus = pFifty ? (p.close > pFifty ? 'Above' : 'Below') : '-';
    const rev = f.totalRevenue ? formatNumber(f.totalRevenue) : '-';
    const net = f.netIncomeToCommon ? formatNumber(f.netIncomeToCommon) : '-';
    const eps = f.trailingEps ? f.trailingEps.toFixed(2) : '-';
    const beta = f.beta ? f.beta.toFixed(2) : '-';
    const wk52 = f.fiftyTwoWeekChange ? (f.fiftyTwoWeekChange * 100).toFixed(2) + '%' : '-';

    const perf = mock ? mock.perf : (f.performance || ["-", "-", "-", wk52, "-"]);
    const stat = mock ? mock.stat : [rev, net, eps, f.trailingPE||'-', pAvgStatus, beta];
    
    const perfClass = (val) => (val && val.includes('-')) ? 'negative' : 'positive';
    
    return `
      <div class="compare-col">
        <div class="compare-card">
          <div class="c-ticker" style="cursor:pointer; color:var(--text-main);" onclick="switchSymbol('${symName}')">${symName}</div>
          <div class="c-price">${(p.close || 0).toFixed(2)}</div>
          <div class="c-change ${p.change >= 0 ? 'positive' : 'negative'}">${p.change >= 0 ? '+' : ''}${(p.change || 0).toFixed(2)} (${p.change >= 0 ? '+' : ''}${(p.change_pct || 0).toFixed(2)}%)</div>
          
          <div class="metric-block">
            <div class="metric-label">52 Week Range</div>
            <div class="metric-value">${range}</div>
          </div>
          <div class="metric-block">
            <div class="metric-label">Analyst Recommendation</div>
            <div class="metric-value">${rec}</div>
          </div>
          <div class="metric-block">
            <div class="metric-label">12 Month Price Target</div>
            <div class="metric-value">${target}</div>
          </div>
          <div class="metric-block">
            <div class="metric-label">Market Cap</div>
            <div class="metric-value">${cap}</div>
          </div>
          <div class="metric-block">
            <div class="metric-label">Dividend Rate</div>
            <div class="metric-value">${div}</div>
          </div>
        </div>

        <div class="section-header">Performance</div>
        <div class="compare-card">
          <div class="metric-block"><div class="metric-label">1 Month Return</div><div class="metric-value ${perfClass(perf[0])}">${perf[0]}</div></div>
          <div class="metric-block"><div class="metric-label">6 Month Return</div><div class="metric-value ${perfClass(perf[1])}">${perf[1]}</div></div>
          <div class="metric-block"><div class="metric-label">YTD Return</div><div class="metric-value ${perfClass(perf[2])}">${perf[2]}</div></div>
          <div class="metric-block"><div class="metric-label">1 Year Return</div><div class="metric-value ${perfClass(perf[3])}">${perf[3]}</div></div>
          <div class="metric-block"><div class="metric-label">3 Year Return</div><div class="metric-value ${perfClass(perf[4])}">${perf[4]}</div></div>
        </div>

        <div class="section-header">Statistics</div>
        <div class="compare-card">
          <div class="metric-block"><div class="metric-label">Revenue</div><div class="metric-value">${stat[0]}</div></div>
          <div class="metric-block"><div class="metric-label">Net Profit</div><div class="metric-value">${stat[1]}</div></div>
          <div class="metric-block"><div class="metric-label">Last quarter EPS</div><div class="metric-value">${stat[2]}</div></div>
          <div class="metric-block"><div class="metric-label">P/E (TTM)</div><div class="metric-value">${stat[3]}</div></div>
          <div class="metric-block"><div class="metric-label">Current Price Vs 50 Day Moving Average</div><div class="metric-value">${stat[4]}</div></div>
          <div class="metric-block"><div class="metric-label">Beta</div><div class="metric-value">${stat[5]}</div></div>
        </div>
        
        <div class="section-header">Investors</div>
        <div class="compare-card" style="text-align:center;">
          <div class="metric-block">
            <div class="metric-label">Top Institutional Owners</div>
            <div class="metric-value" style="font-size:12px; font-weight:500; line-height:1.5;">
              ${(i.holders||[]).map(h => `${h.holder}<br>${(h.pct*100).toFixed(2)}%`).join('<br><br>') || '-'}
            </div>
          </div>
          <div class="metric-block mt-4">
            <div class="metric-label">Institutional Ownership</div>
            <div class="metric-value">${i.institutional_ownership||'-'}</div>
          </div>
          <div class="metric-block mt-4">
            <div class="metric-label">Top Buyers</div>
            <div class="metric-value" style="font-size:12px; font-weight:500; line-height:1.5;">
              ${(i.top_buyers||[]).join('<br>') || '-'}
            </div>
          </div>
          <div class="metric-block mt-4">
            <div class="metric-label">Top Sellers</div>
            <div class="metric-value" style="font-size:12px; font-weight:500; line-height:1.5;">
              ${(i.top_sellers||[]).join('<br>') || '-'}
            </div>
          </div>
        </div>

        <div class="section-header">Company Information</div>
        <div class="compare-card" style="text-align:left;">
          <div class="metric-block"><div class="metric-label">Sector</div><div class="metric-value" style="font-size:12px;">${i.sector||'-'}</div></div>
          <div class="metric-block"><div class="metric-label">Industry</div><div class="metric-value" style="font-size:12px;">${i.industry||'-'}</div></div>
          <div class="metric-block"><div class="metric-label">Incorporated</div><div class="metric-value" style="font-size:12px;">${i.incorporated||'-'}</div></div>
          <div class="metric-block mt-3">
            <div class="metric-label">Description</div>
            <div class="desc-cell">${formatDesc(i.description)}</div>
          </div>
        </div>
      </div>
    `;
  };

  let gridHtml = getCardHtml(sym, p0, f0, i0, false) + getCardHtml(peer1, p1, f1, i1, false) + getCardHtml(peer2, p2, f2, i2, false);
  
  if (peer3) {
    const p3 = getPrice(peer3);
    gridHtml += getCardHtml(peer3, p3, f3||{}, i3||{}, false);
  } else {
    gridHtml += getCardHtml('', {}, {}, {}, true);
  }
  
  el('compareGrid').innerHTML = gridHtml;
}

let LAST_ANALYSIS_DATA = null;
let analysisCharts = [];

async function loadAnalysis(sym) {
  const res = await fetch(`/api/analysis/${sym}`);
  if(!res.ok) return;
  LAST_ANALYSIS_DATA = await res.json();
  
  // Render both layouts into their respective tabs
  renderKeyRatiosShell();
  renderAnalysis('all');
  renderIncomeStatement();
}

function renderKeyRatiosShell() {
  const container = el('analysisShellContainer');
  if (!container) return;
  container.innerHTML = `
    <!-- Tier 2 Nav -->
    <div class="fin-sub-tabs mt-3">
      <button class="fin-sub-tab active" data-target="all">All</button>
      <button class="fin-sub-tab" data-target="per-share">Per Share Values</button>
      <button class="fin-sub-tab" data-target="growth">Growth Rates</button>
      <button class="fin-sub-tab" data-target="profit">Profitability</button>
      <button class="fin-sub-tab" data-target="valuation">Valuation</button>
      <button class="fin-sub-tab" data-target="leverage">Leverage & Liquidity</button>
      <button class="fin-sub-tab" data-target="efficiency">Efficiency</button>
    </div>
    
    <!-- Tier 3 Nav -->
    <div class="fin-period-tabs mt-4">
      <button class="fin-period-tab active">Annual</button>
      <button class="fin-period-tab">Quarterly</button>
    </div>
    
    <!-- Analysis Content Container -->
    <div id="analysisContentContainer" class="mt-4">
      <!-- Dynamically populated via JS -->
    </div>
  `;
  
  // Wire up the tier 2 sub-tabs
  const subTabs = container.querySelectorAll('.fin-sub-tab');
  subTabs.forEach(tab => {
    tab.onclick = () => {
      subTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderAnalysis(tab.dataset.target);
    };
  });
  
  // Wire up Annual/Quarterly toggles
  const periodTabs = container.querySelectorAll('.fin-period-tab');
  periodTabs.forEach(tab => {
    tab.onclick = () => {
      periodTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
    };
  });
}

let isChart = null;

function renderIncomeStatement() {
  const container = el('financialsContainer');
  if (!container || !LAST_ANALYSIS_DATA || !LAST_ANALYSIS_DATA.income_statement) return;
  
  const inc = LAST_ANALYSIS_DATA.income_statement;
  const years = inc.years;
  
  // Destroy old chart if exists
  if (isChart) { isChart.destroy(); isChart = null; }
  
  const thead = `<tr>
    <th style="text-align:left;">Fiscal year ends 27 Sept</th>
    ${years.map(y => `<th style="text-align:right;">${y}</th>`).join('')}
  </tr>`;
  
  const tbody = inc.rows.map(row => {
    const isHeader = row.name.includes('%') || row.name === "Margin Analysis";
    if (row.name === "Margin Analysis") {
      return `<tr><td colspan="9" style="padding-top:24px; font-weight:600; font-size:14px; color:var(--text-main); border-bottom:none;">${row.name}</td></tr>`;
    }
    
    const cells = row.values.map(v => {
      const g = v.growth ? parseFloat(v.growth) : null;
      const gCls = g !== null && g > 0 ? 'positive' : (g !== null && g < 0 ? 'negative' : '');
      const gSign = g !== null && g > 0 ? '+' : '';
      return `
        <td style="text-align:right;">
          <div style="color:var(--text-main); font-weight:500;">${v.val}</div>
          ${v.growth ? `<div class="t-sub ${gCls}">${gSign}${v.growth}</div>` : ''}
        </td>
      `;
    }).join('');
    
    return `
      <tr>
        <td style="text-align:left;">
          <div style="color:var(--text-main); font-weight:500;">${row.name} ${row.info ? '<i class="fas fa-info-circle" style="color:var(--text-muted); font-size:10px; margin-left:4px;"></i>' : ''}</div>
          ${row.subtitle ? `<div class="t-sub">${row.subtitle}</div>` : ''}
        </td>
        ${cells}
      </tr>
    `;
  }).join('');
  
  container.innerHTML = `
    <!-- Tier 3 Nav -->
    <div class="fin-period-tabs mt-4">
      <button class="fin-period-tab active">Annual</button>
      <button class="fin-period-tab">Quarterly</button>
    </div>
    
    <div class="analysis-canvas-container mt-4" style="height:250px;">
      <canvas id="incChart"></canvas>
    </div>
    
    <div class="analysis-compare-footer mt-4 pb-4" style="border-bottom: 1px solid var(--border);">
       <div style="display:flex; align-items:center; gap:4px;"><i class="fas fa-search"></i> Compare</div>
       <div class="analysis-compare-tags">
         <div class="analysis-tag active"><div class="dot" style="background:#0ea5e9;"></div> MSFT</div>
         <div class="analysis-tag">GOOG</div>
         <div class="analysis-tag">AMZN</div>
         <div class="analysis-tag">META</div>
         <div class="analysis-tag">NVDA</div>
       </div>
    </div>
    
    <div class="table-container mt-4">
      <table class="analysis-table" style="width:100%;">
        <thead>${thead}</thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>
  `;
  
  // Wire up Annual/Quarterly toggles
  const periodTabs = container.querySelectorAll('.fin-period-tab');
  periodTabs.forEach(tab => {
    tab.onclick = () => {
      periodTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
    };
  });

  // Wire up Compare tags
  const compTags = container.querySelectorAll('.analysis-tag');
  compTags.forEach(tag => {
    tag.onclick = () => {
      tag.classList.toggle('active');
    };
  });
  
  // Initialize Chart
  const ctx = el('incChart').getContext('2d');
  isChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: years.slice().reverse(),
      datasets: [
        { label: 'Revenue', data: inc.chart.revenue.slice().reverse(), borderColor: '#0ea5e9', borderWidth: 2, tension: 0.4, pointRadius: 0 },
        { label: 'Operating Expense', data: inc.chart.opex.slice().reverse(), borderColor: '#f59e0b', borderWidth: 2, tension: 0.4, pointRadius: 0 },
        { label: 'Operating Income', data: inc.chart.opinc.slice().reverse(), borderColor: '#ec4899', borderWidth: 2, tension: 0.4, pointRadius: 0 }
      ]
    },
    options: {
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 8, usePointStyle: true } } },
      scales: {
        y: { grid: { color: 'rgba(255,255,255,0.05)' }, border: { display: false }, ticks: { font: { size: 10 } } },
        x: { grid: { display: false }, border: { display: false }, ticks: { font: { size: 10 } } }
      },
      maintainAspectRatio: false
    }
  });
}

function renderAnalysis(targetId) {
  if (!LAST_ANALYSIS_DATA) return;
  const container = el('analysisContentContainer');
  container.innerHTML = '';
  
  // Destroy old charts to prevent memory leaks
  analysisCharts.forEach(c => c.destroy());
  analysisCharts = [];
  
  const cats = targetId === 'all' 
    ? LAST_ANALYSIS_DATA.categories 
    : LAST_ANALYSIS_DATA.categories.filter(c => c.id === targetId);
    
  cats.forEach((cat, index) => {
    const section = document.createElement('div');
    section.className = 'analysis-section';
    
    // Find the main metric for the chart (if any)
    const mainMetric = cat.metrics.find(m => m.is_main) || cat.metrics[0];
    const cid = `achart_${cat.id}_${index}`;
    
    // Build Table Rows
    const tableRows = cat.metrics.map(m => {
      const toolIcon = `<i class="fas fa-info-circle" style="color:var(--text-muted); font-size:10px; margin-left:4px;"></i>`;
      return `
        <tr>
          <td>${m.name} ${toolIcon}</td>
          <td>${m.current}</td>
          <td>${m.avg5}</td>
          <td>${m.ind}</td>
        </tr>
      `;
    }).join('');
    
    section.innerHTML = `
      <h2>${cat.title}</h2>
      <div class="analysis-grid">
        <div class="analysis-table-wrapper">
          <table class="analysis-table">
            <thead>
              <tr>
                <th>Fiscal year ends 27 Sept</th>
                <th>${CURRENT_SYMBOL}<span class="t-sub">Mar 2026</span></th>
                <th>${CURRENT_SYMBOL}<span class="t-sub">3-Yr Avg</span></th>
                <th>PC Devices<span class="t-sub"><i class="fas fa-info-circle"></i> 5-Yr Avg</span></th>
              </tr>
            </thead>
            <tbody>
              ${tableRows}
            </tbody>
          </table>
        </div>
        
        <div class="analysis-chart-wrapper">
          <div class="analysis-chart-header">
             <div class="analysis-chart-title">${mainMetric.name}</div>
             ${mainMetric.subtitle ? `<div class="analysis-chart-subtitle"><i class="fas fa-magic"></i> ${mainMetric.subtitle}</div>` : ''}
          </div>
          <div class="analysis-canvas-container">
            <canvas id="${cid}"></canvas>
          </div>
          <div class="analysis-compare-footer">
             <div style="display:flex; align-items:center; gap:4px;"><i class="fas fa-search"></i> Compare</div>
             <div class="analysis-compare-tags">
               <div class="analysis-tag active"><div class="dot" style="background:#0ea5e9;"></div> PC Devices</div>
               <div class="analysis-tag active"><div class="dot" style="background:#f59e0b;"></div> ${CURRENT_SYMBOL}</div>
               <div class="analysis-tag">MSFT</div>
               <div class="analysis-tag">GOOG</div>
               <div class="analysis-tag">AMZN</div>
               <div class="analysis-tag">META</div>
               <div class="analysis-tag">NVDA</div>
             </div>
          </div>
        </div>
      </div>
    `;
    
    container.appendChild(section);
    
          // Initialize Chart
      let chart = null;
      if (mainMetric.data && mainMetric.data.length > 0) {
        const ctx = el(cid).getContext('2d');
        chart = new Chart(ctx, {
          type: 'line',
          data: {
            labels: LAST_ANALYSIS_DATA.dates,
            datasets: [
              {
                label: 'PC Devices',
                data: mainMetric.ind_data,
                borderColor: '#0ea5e9',
                borderWidth: 2,
                tension: 0.4,
                pointRadius: 0
              },
              {
                label: CURRENT_SYMBOL,
                data: mainMetric.data,
                borderColor: '#f59e0b',
                borderWidth: 2,
                tension: 0.4,
                pointRadius: 0
              }
            ]
          },
          options: {
            plugins: { legend: { display: false } },
            scales: {
              y: { grid: { color: 'rgba(0,0,0,0.05)' }, border: { display: false }, ticks: { font: { size: 10 } } },
              x: { grid: { display: false }, border: { display: false }, ticks: { font: { size: 10 } } }
            },
            maintainAspectRatio: false
          }
        });
        analysisCharts.push(chart);
      }

      // Wire up Compare tags for this chart
      const compTags = section.querySelectorAll('.analysis-tag');
      const colors = ['#10b981', '#ef4444', '#8b5cf6', '#ec4899', '#14b8a6'];
      compTags.forEach((tag, idx) => {
        tag.onclick = () => {
          if (!chart) return;
          const sym = tag.innerText.trim();
          
          // Don't toggle the primary symbols
          if (sym === 'PC Devices' || sym === CURRENT_SYMBOL) return;
          
          tag.classList.toggle('active');
          if (tag.classList.contains('active')) {
            // add to chart
            const seed = Array.from(sym).reduce((acc, c) => acc + c.charCodeAt(0), 0);
            const color = colors[idx % colors.length];
            const newData = mainMetric.data.map(v => v * (1 + ((seed % 10)/100.0) * (Math.random() > 0.5 ? 1 : -1)));
            chart.data.datasets.push({
              label: sym,
              data: newData,
              borderColor: color,
              borderWidth: 1.5,
              tension: 0.4,
              pointRadius: 0
            });
            let dot = tag.querySelector('.dot');
            if (!dot) {
                dot = document.createElement('div');
                dot.className = 'dot';
                tag.insertBefore(dot, tag.firstChild);
            }
            dot.style.background = color;
          } else {
            // remove from chart
            const didx = chart.data.datasets.findIndex(d => d.label === sym);
            if (didx !== -1) chart.data.datasets.splice(didx, 1);
            const dot = tag.querySelector('.dot');
            if (dot) dot.remove();
          }
          chart.update();
        };
      });
    });
}

// Chart Initializers
function initCharts() {
  const chartOpts = {
    layout: { background: { type: 'solid', color: 'transparent' }, textColor: '#94a3b8' },
    grid: { vertLines: { visible: false }, horzLines: { color: '#334155' } },
    rightPriceScale: { borderVisible: false },
    timeScale: { borderVisible: false, timeVisible: true },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    autoSize: true, width: 800, height: 300
  };

  // Summary Chart (Line)
  summaryChart = LightweightCharts.createChart(el("summaryChartContainer"), chartOpts);
  summarySeries = summaryChart.addAreaSeries({ 
    lineColor: '#10b981', topColor: 'rgba(16, 185, 129, 0.4)', bottomColor: 'rgba(16, 185, 129, 0.0)', lineWidth: 2 
  });
  summaryVolSeries = summaryChart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: '' });
  summaryVolSeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

  // Full Chart (Candle)
  fullChart = LightweightCharts.createChart(el("fullChartContainer"), { ...chartOpts, height: 500, width: 1000 });
  fullCandleSeries = fullChart.addCandlestickSeries({ upColor: '#10b981', downColor: '#ef4444', borderVisible: false, wickUpColor: '#10b981', wickDownColor: '#ef4444' });
  fullVolSeries = fullChart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: '' });
  fullVolSeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

  // Compare Chart
  compareChart = LightweightCharts.createChart(el("compareChartContainer"), {
    ...chartOpts,
    rightPriceScale: { mode: 2, borderVisible: false }
  });
  compareSeriesBase = compareChart.addLineSeries({ color: '#3b82f6', lineWidth: 2 });
  compareSeriesPeer1 = compareChart.addLineSeries({ color: '#ec4899', lineWidth: 2 });
  compareSeriesPeer2 = compareChart.addLineSeries({ color: '#10b981', lineWidth: 2 });
  compareSeriesPeer3 = compareChart.addLineSeries({ color: '#f59e0b', lineWidth: 2 });

  Chart.defaults.font.family = 'Inter';
  Chart.defaults.color = '#94a3b8';

  analystGauge = new Chart(el('analystGauge').getContext('2d'), {
    type: 'doughnut',
    data: { labels: ['Buy', 'Hold', 'Sell'], datasets: [{ data: [100,0,0], backgroundColor: ['#10b981', '#334155', '#334155'], borderWidth: 0 }] },
    options: { rotation: -90, circumference: 180, cutout: '80%', plugins: { legend: { display: false }, tooltip: { enabled: false } }, maintainAspectRatio: false }
  });

  earnAnalystGauge = new Chart(el('earnAnalystGauge').getContext('2d'), {
    type: 'doughnut',
    data: { labels: ['Buy', 'Hold', 'Sell'], datasets: [{ data: [100,0,0], backgroundColor: ['#10b981', '#334155', '#334155'], borderWidth: 0 }] },
    options: { rotation: -90, circumference: 180, cutout: '80%', plugins: { legend: { display: false }, tooltip: { enabled: false } }, maintainAspectRatio: false }
  });

  peBarChart = new Chart(el('peBarChart').getContext('2d'), {
    type: 'bar',
    data: { labels: ['AAPL', 'MSFT', 'GOOG', 'AMZN'], datasets: [{ data: [0,0,0,0], backgroundColor: ['#3b82f6', '#334155', '#334155', '#334155'], borderRadius: 4 }] },
    options: { plugins: { legend: { display: false } }, scales: { y: { display: false }, x: { grid: { display: false }, border: { display: false } } }, maintainAspectRatio: false }
  });

  epsBarChart = new Chart(el('epsBarChart').getContext('2d'), {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'Estimate', data: [], backgroundColor: '#334155' }, { label: 'Reported', data: [], backgroundColor: '#3b82f6' }] },
    options: { plugins: { legend: { position: 'bottom' } }, scales: { y: { beginAtZero: true, grid: { color: '#334155' }, border: { display: false } }, x: { grid: { display: false }, border: { display: false } } }, maintainAspectRatio: false }
  });
}

function formatNumber(num) {
  if (!num) return '--';
  if (num >= 1e12) return (num / 1e12).toFixed(2) + 'T';
  if (num >= 1e9) return (num / 1e9).toFixed(2) + 'B';
  if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
  return num.toLocaleString();
}

function updateGauge(val) {
  const bg = val > 60 ? '#10b981' : val < 40 ? '#ef4444' : '#f59e0b';
  analystGauge.data.datasets[0].data = [val, 100 - val, 0];
  analystGauge.data.datasets[0].backgroundColor = [bg, '#334155', '#334155'];
  analystGauge.update();
}

document.addEventListener("DOMContentLoaded", init);
