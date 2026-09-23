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
let analystGauge, peBarChart, compareChart;
let compareSeriesBase, compareSeriesPeer1, compareSeriesPeer2, compareSeriesPeer3;
let customCompareSymbol = null;

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
  await loadPipelineHealth();
  initCharts();
  initTabs();
  initSubTabs();
  initSearch();
  switchSymbol(CURRENT_SYMBOL);
  setInterval(fetchTickers, 60000);
  setInterval(loadPipelineHealth, 10000);
}

async function loadPipelineHealth() {
  const container = el('pipelineHealth');
  if (!container) return;
  try {
    const [healthResponse, lagResponse] = await Promise.all([
      fetch('/api/health'),
      fetch('/api/consumer-lag')
    ]);
    const health = healthResponse.ok ? await healthResponse.json() : { services: [] };
    const lag = lagResponse.ok ? await lagResponse.json() : { consumer_lag: null };
    const chips = (health.services || []).map(service =>
      `<span class="health-chip ${service.status}">${service.service}: ${service.status}</span>`
    );
    const lagLabel = lag.consumer_lag === null ? 'lag: unavailable' : `lag: ${Math.round(lag.consumer_lag)}`;
    chips.push(`<span class="health-chip ${lag.consumer_lag === null ? 'pending' : 'ok'}">${lagLabel}</span>`);
    container.innerHTML = chips.join('');
  } catch (error) {
    container.innerHTML = '<span class="health-chip error">health unavailable</span>';
  }
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
  el("headerSymbol").innerText = CURRENT_SYMBOL;
  
  // Re-connect SSE stream
  if (eventSource) eventSource.close();
  eventSource = new EventSource(`/stream/${CURRENT_SYMBOL}`);
  eventSource.onmessage = handleStreamMessage;
  
  // Load data for all tabs
  loadFundamentals(CURRENT_SYMBOL);
  loadNews(CURRENT_SYMBOL);
  loadFinancials(CURRENT_SYMBOL);
  loadCompanyInfo(CURRENT_SYMBOL);
  loadCompare(CURRENT_SYMBOL);
  
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
  
  peBarChart.data.datasets[0].data = [d.trailingPE || null];
  peBarChart.update();
}

async function loadNews(sym) {
  const res = await fetch(`/api/news/${sym}`);
  if(!res.ok) return;
  const news = await res.json();
  const container = el('newsContainer');
  if(!container) return;
  
  container.innerHTML = news.map(n => {
    const timestamp = typeof n.time === 'number' ? n.time * 1000 : n.time;
    const timeStr = timestamp ? new Date(timestamp).toLocaleDateString() : 'Date unavailable';
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
  if(!res.ok) {
    const container = el('financialsContainer');
    if (container) {
      container.innerHTML = '<div class="data-unavailable">Verified financial statements are unavailable from the configured provider.</div>';
    }
    return;
  }
  const payload = await res.json();
  const d = payload.periods || payload;
  
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
    
    const p = p_live;
    const range = `${f.fiftyTwoWeekLow||0} - ${f.fiftyTwoWeekHigh||0}`;
    const rec = f.recommendation||'Unavailable';
    const target = f.targetMeanPrice||'-';
    const cap = formatNumber(f.marketCap);
    const div = `${((f.dividendYield||0)*100).toFixed(2)}%`;
    
    const pFifty = f.fiftyDayAverage;
    const pAvgStatus = pFifty ? (p.close > pFifty ? 'Above' : 'Below') : '-';
    const rev = f.totalRevenue ? formatNumber(f.totalRevenue) : '-';
    const net = f.netIncomeToCommon ? formatNumber(f.netIncomeToCommon) : '-';
    const eps = f.trailingEps ? f.trailingEps.toFixed(2) : '-';
    const beta = f.beta ? f.beta.toFixed(2) : '-';
    const wk52 = f.fiftyTwoWeekChange ? (f.fiftyTwoWeekChange * 100).toFixed(2) + '%' : '-';

    const perf = f.performance || ["-", "-", "-", wk52, "-"];
    const stat = [rev, net, eps, f.trailingPE||'-', pAvgStatus, beta];
    
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

  peBarChart = new Chart(el('peBarChart').getContext('2d'), {
    type: 'bar',
    data: { labels: ['Current'], datasets: [{ data: [null], backgroundColor: ['#3b82f6'], borderRadius: 4 }] },
    options: { plugins: { legend: { display: false } }, scales: { y: { display: false }, x: { grid: { display: false }, border: { display: false } } }, maintainAspectRatio: false }
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
