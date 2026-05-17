/* ═══════════════════════════════════════════════════════════
   RV4 — Proyecciones al Cierre v3
   proyecciones.js
   ═══════════════════════════════════════════════════════════ */

const API = '';

// ─────────────────────────────────────────────
//  AUTH
// ─────────────────────────────────────────────
(function () {
  const token   = localStorage.getItem('token');
  const usuario = JSON.parse(localStorage.getItem('usuario') || 'null');
  if (!token || !usuario) { window.location.href = '/index.html'; return; }
  const el = id => document.getElementById(id);
  if (el('userName'))   el('userName').textContent   = usuario.nombre || '—';
  if (el('userRole'))   el('userRole').textContent   = (usuario.rol || '').replace(/_/g, ' ');
  if (el('userAvatar')) el('userAvatar').textContent = (usuario.nombre || 'A')[0].toUpperCase();
  if (el('envLabel'))   el('envLabel').textContent   = location.hostname === 'localhost' ? 'DESARROLLO' : 'PRODUCCIÓN';
  window._token   = token;
  window._usuario = usuario;
})();
function logout() { localStorage.clear(); location.href = '/index.html'; }

// ─────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────
const fmtQ = n => {
  const v = Number(n || 0);
  return 'Q ' + v.toLocaleString('es-GT', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
};
const fmtQM = n => {
  const v = Number(n || 0);
  if (Math.abs(v) >= 1e6) return 'Q ' + (v / 1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return 'Q ' + (v / 1e3).toFixed(0) + 'K';
  return 'Q ' + v.toFixed(0);
};
const fmtPct = n => Number(n || 0).toFixed(1) + '%';
const fmtNum = n => Number(n || 0).toLocaleString('es-GT');
function esc(s) { return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;'); }
function setText(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
function setColor(id, color) { const el = document.getElementById(id); if (el) el.style.color = color; }

async function apiFetch(path, opts = {}) {
  const token = localStorage.getItem('token');
  const res = await fetch(API + path, {
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json', ...opts.headers },
    ...opts
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

function toast(msg, type = 'green') {
  const t = document.getElementById('toastEl');
  t.textContent = msg;
  t.style.background = type === 'green' ? '#16a34a' : type === 'amber' ? '#d97706' : '#b91c1c';
  t.className = 'toast show';
  clearTimeout(window._toast);
  window._toast = setTimeout(() => t.className = 'toast', 4000);
}

function addMonths(fecha, n) {
  const d = new Date(fecha);
  d.setMonth(d.getMonth() + n);
  return d;
}
function formatMes(d) {
  return d.toLocaleDateString('es-GT', { month: 'short', year: 'numeric' });
}

// ─────────────────────────────────────────────
//  ESTADO
// ─────────────────────────────────────────────
const S = {
  empresa: '',
  supuestos: null,
  ingresos: null,
  egresosOp: null,
  egresosFinancieros: null,
  tierraDividendos: null,
  flujo: null,
  plazosVenta: [],        // [{plazo, unidMes, ticket, tasa, inicio, fin, unidProy}]
  anos: 5,
  tasaDesc: 0.12,
  pctISR: 0,
};

// ─────────────────────────────────────────────
//  INIT
// ─────────────────────────────────────────────
async function initProyectos() {
  try {
    const lista = await apiFetch('/api/proyecciones/');
    const sel = document.getElementById('selProyecto');
    lista.sort((a, b) => a.proyecto.localeCompare(b.proyecto));
    lista.forEach(p => {
      const opt = document.createElement('option');
      opt.value = p.empresa;
      opt.textContent = `${p.proyecto} — ${p.empresa}`;
      sel.appendChild(opt);
    });
  } catch (e) {
    toast('Error cargando proyectos: ' + e.message, 'red');
  }
}

async function onProyectoChange() {
  const sel = document.getElementById('selProyecto');
  S.empresa = sel.value;
  if (!S.empresa) {
    document.getElementById('mainContent').style.display = 'none';
    return;
  }
  document.getElementById('mainContent').style.display = 'block';
  document.getElementById('savedBadge').style.display = 'none';
  document.getElementById('projMeta').textContent = `Empresa: ${S.empresa}`;
  S.plazosVenta = [];
  resetUI();
  await Promise.all([cargarSupuestos(), cargarIngresos(), cargarEgresosOp(), cargarFinancieros(), cargarTierra()]);
}

// ─────────────────────────────────────────────
//  SUPUESTOS
// ─────────────────────────────────────────────
async function cargarSupuestos() {
  try {
    const d = await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}`);
    if (d.existe) {
      document.getElementById('inpAnos').value    = d.anos_proyecto || 5;
      document.getElementById('inpDesc').value    = d.tasa_descuento ? +(d.tasa_descuento * 100).toFixed(2) : 12;
      document.getElementById('inpISR').value     = d.pct_isr ? +(d.pct_isr * 100).toFixed(2) : '';
      if (d.anos_ic && document.getElementById('inpAnosIC')) document.getElementById('inpAnosIC').value = d.anos_ic;
      if (d.anos_egr && document.getElementById('inpAnosEgr')) document.getElementById('inpAnosEgr').value = d.anos_egr;
      S.anos     = d.anos_proyecto || 5;
      S.tasaDesc = d.tasa_descuento || 0.12;
      S.pctISR   = d.pct_isr || 0;
      S.plazosVenta = Array.isArray(d.plazos_venta) ? d.plazos_venta : [];
      document.getElementById('savedBadge').style.display = 'inline';
      document.getElementById('projMeta').textContent =
        `Empresa: ${S.empresa} · Guardado por ${d.actualizado_por || '—'} el ${d.actualizado_en ? d.actualizado_en.split('T')[0] : '—'}`;
    }
    renderPlazosVenta();
  } catch (e) {
    renderPlazosVenta();
  }
}

async function guardarTodo() {
  if (!S.empresa) return;
  syncPlazosDOM();
  const body = {
    anos_proyecto:    parseInt(document.getElementById('inpAnos').value) || 5,
    tasa_descuento:   parseFloat(document.getElementById('inpDesc').value) / 100 || 0.12,
    pct_isr:          parseFloat(document.getElementById('inpISR').value) / 100 || 0,
    anos_ic:          parseInt(document.getElementById('inpAnosIC')?.value) || 0,
    anos_egr:         parseInt(document.getElementById('inpAnosEgr')?.value) || 0,
    plazos_venta:     S.plazosVenta,
    prestamos_manual: [],
    pagos_tierra_manual: [],
  };
  try {
    await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}`, {
      method: 'POST', body: JSON.stringify(body),
    });
    document.getElementById('savedBadge').style.display = 'inline';
    toast('✓ Supuestos guardados', 'green');
  } catch (e) {
    toast('Error guardando: ' + e.message, 'red');
  }
}

// ─────────────────────────────────────────────
//  1. INGRESOS
// ─────────────────────────────────────────────
async function cargarIngresos() {
  try {
    const d = await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}/ingresos`);
    S.ingresos = d;
    const r = d.ingresos_reales;
    setText('ingRealContratos',  `${fmtNum(r.contratos)} contratos activos`);
    setText('ingRealSaldo',      fmtQ(r.saldo_pendiente));
    setText('ingRealCapital',    `Capital: ${fmtQ(r.saldo_capital)}`);
    setText('ingRealInteres',    `Intereses: ${fmtQ(r.saldo_interes)}`);
    setText('ingDisponibles',    `${fmtNum(d.total_disponibles)} lotes disponibles`);
    setText('ingPrecioPromedio', `Precio prom: ${fmtQ(d.precio_promedio)}`);
    renderPlazosVenta();
  } catch (e) {
    toast('Error cargando ingresos: ' + e.message, 'red');
  }
}

// ─────────────────────────────────────────────
//  TABLA DE PLAZOS DE VENTA
// ─────────────────────────────────────────────
// Plazos disponibles para el selector
const PLAZOS_OPCIONES = ['Contado','10','12','18','24','36','48','60','72','84','96','120'];

function agregarPlazo() {
  const hoy = new Date();
  const mesInicio = hoy.toISOString().slice(0,7);
  S.plazosVenta.push({ plazo: '60', unidMes: 0, ticket: 0, tasa: 0, inicio: mesInicio, fin: '', unidProy: 0 });
  renderPlazosVenta();
}

function plazoMeses(plazo) {
  // Retorna número de meses del plazo (0 para Contado)
  if (!plazo || plazo === 'Contado') return 0;
  return parseInt(plazo) || 0;
}

function calcularFechaFin(inicio, unidMes, totalDisp) {
  if (!inicio || !unidMes || unidMes <= 0 || !totalDisp) return '';
  const meses = Math.ceil(totalDisp / unidMes);
  const d = new Date(inicio + '-01');
  d.setMonth(d.getMonth() + meses);
  return d.toISOString().slice(0,7);
}

function calcularUnidades(inicio, fin, unidMes) {
  if (!inicio || !fin || !unidMes) return 0;
  const ini = new Date(inicio + '-01');
  const fi  = new Date(fin   + '-01');
  const meses = Math.max(0, (fi.getFullYear() - ini.getFullYear()) * 12 + (fi.getMonth() - ini.getMonth()));
  return unidMes * meses;
}

function renderPlazosVenta() {
  const tbody = document.getElementById('tbPlazos');
  const totalDisp = S.ingresos ? S.ingresos.total_disponibles : 0;

  if (!S.plazosVenta.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:16px;color:var(--muted)">Agregá al menos un plazo de venta.</td></tr>';
    setText('totalIngProy', '—'); setText('totalUnidProy', '—');
    return;
  }

  tbody.innerHTML = S.plazosVenta.map((p, i) => {
    const esContado = p.plazo === 'Contado';
    const plazoM = plazoMeses(p.plazo);
    const tk   = parseFloat(p.ticket) || 0;
    const tasa = esContado ? 0 : (parseFloat(p.tasa) || 0);
    const unidMes = parseInt(p.unidMes) || 0;
    const finStr = p.fin || '';
    const unidProy = finStr ? calcularUnidades(p.inicio, finStr, unidMes) : 0;
    const intUnit  = esContado ? 0 : tk * (tasa/100) * (plazoM/12);
    const totPlazo = (tk + intUnit) * unidProy;
    S.plazosVenta[i].unidProy = unidProy;

    const opcionesPlazo = PLAZOS_OPCIONES.map(op =>
      `<option value="${op}" ${op === p.plazo ? 'selected' : ''}>${op}${op !== 'Contado' ? 'm' : ''}</option>`
    ).join('');

    return `<tr data-idx="${i}">
      <td>
        <select style="width:90px;padding:7px 6px;font-size:12px;font-weight:600;border:none;background:transparent;font-family:inherit"
          onchange="S.plazosVenta[${i}].plazo=this.value;if(this.value==='Contado'){S.plazosVenta[${i}].tasa=0;}renderPlazosVenta()">
          ${opcionesPlazo}
        </select>
      </td>
      <td><input type="number" placeholder="0" value="${p.unidMes||''}" min="0" style="text-align:right"
          oninput="S.plazosVenta[${i}].unidMes=+this.value"></td>
      <td><input type="number" placeholder="0" value="${p.ticket||''}" min="0" step="1000" style="text-align:right"
          oninput="S.plazosVenta[${i}].ticket=+this.value"></td>
      <td><input type="number" placeholder="${esContado?'N/A':'0'}" value="${esContado?'':p.tasa||''}"
          min="0" max="100" step="0.5" style="text-align:right"
          ${esContado?'disabled style="background:#f0f0f0;cursor:not-allowed;text-align:right"':''}
          oninput="S.plazosVenta[${i}].tasa=+this.value"></td>
      <td><input type="month" value="${p.inicio||''}"
          oninput="S.plazosVenta[${i}].inicio=this.value;S.plazosVenta[${i}].fin='';renderPlazosVenta()"></td>
      <td><input type="month" value="${finStr}"
          oninput="S.plazosVenta[${i}].fin=this.value;renderPlazosVenta()"></td>
      <td style="text-align:right;padding:8px 10px;font-weight:700;color:var(--blue)">${fmtNum(unidProy)}</td>
      <td style="text-align:right;padding:8px 10px;font-weight:700;color:var(--green)">${fmtQ(totPlazo)}</td>
      <td style="text-align:center"><button class="del-btn" onclick="eliminarPlazo(${i})">✕</button></td>
    </tr>`;
  }).join('');

  actualizarKPIsPlazos();
}

function eliminarPlazo(i) { S.plazosVenta.splice(i,1); renderPlazosVenta(); }

function calcularPlazos() {
  // Botón Calcular: distribuye unidades disponibles entre los plazos activos
  // según unid/mes de cada plazo. La fecha inicio ya está puesta. Calcula la fecha fin de cada uno.
  const totalDisp = S.ingresos ? S.ingresos.total_disponibles : 0;
  if (!totalDisp || !S.plazosVenta.length) return;

  // Total unidades por mes sumando todos los plazos
  const totalUnidMes = S.plazosVenta.reduce((s, p) => s + (parseInt(p.unidMes) || 0), 0);
  if (totalUnidMes === 0) { toast('Completá las unidades por mes en cada plazo', 'amber'); return; }

  // Calcular fecha fin de cada plazo: cuántos meses tarda en vender su proporción
  let unidRestantes = totalDisp;
  S.plazosVenta.forEach((p, i) => {
    const uMes = parseInt(p.unidMes) || 0;
    if (uMes <= 0) return;
    // Meses necesarios = unidades restantes / unidades mes TOTAL (porque todos venden en paralelo)
    const mesesNec = Math.ceil(unidRestantes / totalUnidMes);
    const ini = p.inicio || new Date().toISOString().slice(0,7);
    const d = new Date(ini + '-01');
    d.setMonth(d.getMonth() + mesesNec);
    S.plazosVenta[i].fin = d.toISOString().slice(0,7);
    // Unidades de este plazo = uMes * mesesNec
    S.plazosVenta[i].unidProy = uMes * mesesNec;
  });

  renderPlazosVenta();
  toast('✓ Fechas calculadas', 'green');
}

function actualizarKPIsPlazos() {
  const totalDisp = S.ingresos ? S.ingresos.total_disponibles : 0;
  let totalCap = 0, totalInt = 0, totalUnid = 0;
  S.plazosVenta.forEach(p => {
    const tk = parseFloat(p.ticket) || 0;
    const esContado = p.plazo === 'Contado';
    const tasa = esContado ? 0 : (parseFloat(p.tasa) || 0);
    const plazoM = plazoMeses(p.plazo);
    const unid = p.unidProy || 0;
    totalCap += tk * unid;
    totalInt += tk * (tasa/100) * (plazoM/12) * unid;
    totalUnid += unid;
  });

  // KPIs de ingresos proyectados
  setText('kpiProyTotal',   fmtQ(totalCap + totalInt));
  setText('kpiProyCapital', fmtQ(totalCap));
  setText('kpiProyInteres', fmtQ(totalInt));
  setText('kpiProyUnid',    fmtNum(totalUnid));

  setText('totalIngProy',  fmtQ(totalCap + totalInt));
  setText('totalUnidProy', `${fmtNum(totalUnid)} / ${fmtNum(totalDisp)} lotes`);
  const diff = totalDisp - totalUnid;
  const diffEl = document.getElementById('diffUnidades');
  if (diffEl) {
    diffEl.textContent = diff === 0 ? '✓ Cuadra exacto' : diff > 0 ? `${diff} lotes sin asignar` : `${Math.abs(diff)} lotes excedidos`;
    diffEl.style.color = diff === 0 ? 'var(--green)' : diff > 0 ? 'var(--amber)' : 'var(--red)';
  }
}

function syncPlazosDOM() {
  document.querySelectorAll('#tbPlazos tr[data-idx]').forEach(tr => {
    const i = +tr.dataset.idx;
    const sel = tr.querySelector('select');
    const inputs = tr.querySelectorAll('input');
    if (S.plazosVenta[i]) {
      S.plazosVenta[i].plazo   = sel?.value || S.plazosVenta[i].plazo;
      S.plazosVenta[i].unidMes = +inputs[0]?.value || 0;
      S.plazosVenta[i].ticket  = +inputs[1]?.value || 0;
      S.plazosVenta[i].tasa    = +inputs[2]?.value || 0;
      S.plazosVenta[i].inicio  = inputs[3]?.value || '';
      S.plazosVenta[i].fin     = inputs[4]?.value || '';
    }
  });
}

// ─────────────────────────────────────────────
//  2. EGRESOS OPERATIVOS
// ─────────────────────────────────────────────
async function cargarEgresosOp() {
  try {
    const anosEgr = parseInt(document.getElementById('inpAnosEgr')?.value) || 0;
    const url = `/api/proyecciones/${encodeURIComponent(S.empresa)}/egresos-operativos?anos_egr=${anosEgr}`;
    const d = await apiFetch(url);
    S.egresosOp = d;
    renderEgresosOp(d);
  } catch (e) {
    toast('Error cargando egresos operativos: ' + e.message, 'red');
  }
}

function renderEgresosOp(d) {
  const renderSeccion = (data, label) => `
    <div class="egr-seccion">
      <div class="egr-label">${label}</div>
      <div class="egr-bars">
        <div class="egr-bar-row">
          <span class="egr-bar-lbl">Presupuesto</span>
          <div class="egr-bar-wrap"><div class="egr-bar" style="width:100%;background:var(--borde)"></div></div>
          <span class="egr-val">${fmtQ(data.presupuesto)}</span>
        </div>
        <div class="egr-bar-row">
          <span class="egr-bar-lbl">Ejecutado</span>
          <div class="egr-bar-wrap"><div class="egr-bar" style="width:${Math.min(data.avance_pct,100)}%;background:var(--amber)"></div></div>
          <span class="egr-val">${fmtQ(data.ejecutado)} <small style="color:var(--muted)">(${fmtPct(data.avance_pct)})</small></span>
        </div>
        <div class="egr-bar-row">
          <span class="egr-bar-lbl" style="color:var(--red)">Pendiente</span>
          <div class="egr-bar-wrap"><div class="egr-bar" style="width:${Math.min(100-data.avance_pct,100)}%;background:var(--red)"></div></div>
          <span class="egr-val" style="color:var(--red)">${fmtQ(data.pendiente)}</span>
        </div>
        ${data.dist_anual ? `<div class="egr-bar-row">
          <span class="egr-bar-lbl" style="color:var(--blue)">/ año</span>
          <div class="egr-bar-wrap"></div>
          <span class="egr-val" style="color:var(--blue);font-weight:700">${fmtQ(data.dist_anual)}</span>
        </div>` : ''}
      </div>
    </div>`;

  const container = document.getElementById('egresosOpContainer');
  if (!container) return;
  const anosEgr = d.anos_distribucion || 0;
  container.innerHTML = `
    ${renderSeccion(d.urbanizacion, '🏗️ Urbanización + Movimiento de Tierras')}
    ${renderSeccion(d.administracion, '🏢 Administración')}
    <div class="egr-total-row">
      <span>Total Presupuesto: <strong>${fmtQ(d.total.presupuesto)}</strong></span>
      <span>Ejecutado: <strong style="color:var(--amber)">${fmtQ(d.total.ejecutado)}</strong></span>
      <span>Pendiente: <strong style="color:var(--red)">${fmtQ(d.total.pendiente)}</strong></span>
      <span>Avance: <strong>${fmtPct(d.total.avance_pct)}</strong></span>
      ${d.total.dist_anual ? `<span>Por año: <strong style="color:var(--blue)">${fmtQ(d.total.dist_anual)}</strong></span>` : ''}
    </div>`;
}

// ─────────────────────────────────────────────
//  3. EGRESOS FINANCIEROS
// ─────────────────────────────────────────────
async function cargarFinancieros() {
  try {
    const anosIc = parseInt(document.getElementById('inpAnosIC')?.value) || 0;
    const url = `/api/proyecciones/${encodeURIComponent(S.empresa)}/egresos-financieros?anos_ic=${anosIc}`;
    const d = await apiFetch(url);
    S.egresosFinancieros = d;
    renderFinancieros(d);
  } catch (e) {
    toast('Error cargando egresos financieros: ' + e.message, 'red');
  }
}

function renderFinancieros(d) {
  const prestEl = document.getElementById('prestamoBancarioContainer');
  const icEl    = document.getElementById('intercompanyContainer');

  if (prestEl) {
    const p = d.prestamo_bancario;
    if (!p) {
      prestEl.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:12px">No hay préstamo bancario registrado para este proyecto.</div>';
    } else {
      const pct_pag = p.monto_original > 0 ? (p.pagado_capital / p.monto_original * 100).toFixed(1) : 0;
      // KPI superiores: pagado REAL del flujo (no de tabla de amortización)
      const capitalPagadoReal = (p.pagado_cuota_capital || 0) + (p.pagado_liberaciones || 0);
      const interesesPagadoReal = p.pagado_intereses || 0;
      const capitalPendienteReal = p.monto_original - capitalPagadoReal;
      const interesesPendientes = p.pendiente_interes || 0;
      prestEl.innerHTML = `
        <div class="prest-header">
          <div><span class="badge-info">${esc(p.banco)}</span> <span class="badge-info">No. ${esc(p.no_credito)}</span></div>
          <div style="font-size:11px;color:var(--muted)">Monto original: ${fmtQ(p.monto_original)} · Tasa: ${(p.tasa*100).toFixed(2)}%</div>
        </div>
        <div class="prest-kpis">
          <div class="kpi green"><div class="kpi-val">${fmtQ(capitalPagadoReal)}</div><div class="kpi-lbl">Capital pagado</div><div class="kpi-sub">${p.cuotas_pagadas} cuotas · ${pct_pag}%</div></div>
          <div class="kpi amber"><div class="kpi-val">${fmtQ(interesesPagadoReal)}</div><div class="kpi-lbl">Intereses pagados</div></div>
          <div class="kpi red"><div class="kpi-val">${fmtQ(capitalPendienteReal)}</div><div class="kpi-lbl">Capital pendiente</div><div class="kpi-sub">${p.cuotas_pendientes.length} cuotas</div></div>
          <div class="kpi red"><div class="kpi-val">${fmtQ(interesesPendientes)}</div><div class="kpi-lbl">Intereses pendientes</div></div>
        </div>
        <button class="amort-toggle" onclick="toggleAmort('amortTableWrap')">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" style="width:12px;height:12px"><polyline points="6 9 12 15 18 9"/></svg>
          Ver tabla de amortización pendiente (${p.cuotas_pendientes.length} cuotas)
        </button>
        <div class="amort-table-wrap" id="amortTableWrap">
          <table class="amort-table">
            <thead><tr><th style="text-align:left">Cuota</th><th>Fecha</th><th>Saldo capital</th><th>Capital</th><th>Interés</th><th>Total cuota</th></tr></thead>
            <tbody>
              ${p.cuotas_pendientes.map(c => `<tr>
                <td style="text-align:left;font-weight:600">Op. ${c.op}</td>
                <td>${c.fecha}</td>
                <td>${fmtQ(c.saldo_capital)}</td>
                <td>${fmtQ(c.capital)}</td>
                <td style="color:var(--amber)">${fmtQ(c.interes)}</td>
                <td style="font-weight:700">${fmtQ(c.cuota)}</td>
              </tr>`).join('')}
              <tr style="background:var(--azul);color:#fff;font-weight:700">
                <td colspan="3" style="padding:8px 10px">TOTAL PENDIENTE</td>
                <td>${fmtQ(p.pendiente_capital)}</td>
                <td>${fmtQ(p.pendiente_interes)}</td>
                <td>${fmtQ(p.pendiente_total)}</td>
              </tr>
            </tbody>
          </table>
        </div>`;
    }
  }

  // ── Cuadre ejecutado real vs tabla + proyección por año ──────────
  const ejec = d.ejecutado_financiamiento || {};
  const porAnio = d.cuotas_por_anio || {};
  const aniosKeys = Object.keys(porAnio).sort();
  const p = d.prestamo_bancario;

  if (prestEl && p) {
    const capitalPagado    = (p.pagado_cuota_capital || 0) + (p.pagado_liberaciones || 0);
    const interesesPagados = p.pagado_intereses || 0;
    const capitalTabla     = p.pagado_capital  || 0;
    const interesesTabla   = p.pagado_interes  || 0;
    const diffCapital      = capitalPagado  - capitalTabla;
    const diffIntereses    = interesesPagados - interesesTabla;

    const fmtDiff = (d) => {
      if (Math.abs(d) < 500) return `<span style="color:var(--green)">✓ Cuadra</span>`;
      if (d > 0) return `<span style="color:var(--amber)">+${fmtQ(d)} anticipado</span>`;
      return `<span style="color:var(--red)">${fmtQ(Math.abs(d))} pendiente</span>`;
    };

    const cuadreHtml = `
      <div style="margin-top:16px;border:1px solid var(--borde);border-radius:8px;padding:14px 16px;background:var(--gris)">
        <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:12px">
          Cruce pagos reales vs tabla de amortización
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px">
          <thead><tr style="background:var(--azul);color:#fff">
            <th style="padding:7px 10px;text-align:left">Concepto</th>
            <th style="padding:7px 10px;text-align:right">Pagado real (flujo)</th>
            <th style="padding:7px 10px;text-align:right">Tabla amortización</th>
            <th style="padding:7px 10px;text-align:right">Diferencia</th>
          </tr></thead>
          <tbody>
            <tr style="border-bottom:1px solid var(--borde-lt)">
              <td style="padding:6px 10px;font-weight:600">Capital<br><small style="color:var(--muted);font-weight:400">Cuota Capital + Liberaciones</small></td>
              <td style="padding:6px 10px;text-align:right;font-weight:700">${fmtQ(capitalPagado)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(capitalTabla)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtDiff(diffCapital)}</td>
            </tr>
            <tr style="border-bottom:1px solid var(--borde-lt)">
              <td style="padding:6px 10px;font-weight:600">Intereses Préstamo</td>
              <td style="padding:6px 10px;text-align:right;font-weight:700">${fmtQ(interesesPagados)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(interesesTabla)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtDiff(diffIntereses)}</td>
            </tr>
            <tr style="background:var(--gris2,rgba(0,0,0,.04));font-weight:700">
              <td style="padding:6px 10px">TOTAL</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(capitalPagado + interesesPagados)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(capitalTabla + interesesTabla)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtDiff(diffCapital + diffIntereses)}</td>
            </tr>
          </tbody>
        </table>
        ${aniosKeys.length ? `
        <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px">
          Proyección cuotas pendientes por año
        </div>
        <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:11px;min-width:450px">
            <thead><tr style="background:var(--azul);color:#fff">
              <th style="padding:6px 10px;text-align:left">Año</th>
              <th style="padding:6px 10px;text-align:center">Cuotas</th>
              <th style="padding:6px 10px;text-align:right">Capital</th>
              <th style="padding:6px 10px;text-align:right">Intereses</th>
              <th style="padding:6px 10px;text-align:right">Total cuota</th>
            </tr></thead>
            <tbody>
              ${aniosKeys.map(yr => {
                const data = porAnio[yr];
                return `<tr style="border-bottom:1px solid var(--borde-lt)">
                  <td style="padding:5px 10px;font-weight:700;color:var(--azul)">${yr}</td>
                  <td style="padding:5px 10px;text-align:center;color:var(--muted)">${data.n}</td>
                  <td style="padding:5px 10px;text-align:right">${fmtQ(data.capital)}</td>
                  <td style="padding:5px 10px;text-align:right;color:var(--amber)">${fmtQ(data.interes)}</td>
                  <td style="padding:5px 10px;text-align:right;font-weight:700">${fmtQ(data.cuota)}</td>
                </tr>`;
              }).join('')}
            </tbody>
            <tfoot><tr style="background:var(--azul);color:#fff;font-weight:700">
              <td style="padding:6px 10px">TOTAL PENDIENTE</td>
              <td style="padding:6px 10px;text-align:center">${Object.values(porAnio).reduce((s,d)=>s+d.n,0)}</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(Object.values(porAnio).reduce((s,d)=>s+d.capital,0))}</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(Object.values(porAnio).reduce((s,d)=>s+d.interes,0))}</td>
              <td style="padding:6px 10px;text-align:right">${fmtQ(Object.values(porAnio).reduce((s,d)=>s+d.cuota,0))}</td>
            </tr></tfoot>
          </table>
        </div>` : '<div style="font-size:11px;color:var(--muted)">Sin cuotas pendientes registradas</div>'}
      </div>`;

    prestEl.innerHTML += cuadreHtml;
  }

    if (icEl) {
    const ic = d.intercompany;
    icEl.innerHTML = `
      <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-start">
        <div class="kpi ${ic.tipo === 'por pagar' ? 'red' : 'green'}" style="min-width:200px">
          <div class="kpi-val">${fmtQ(ic.saldo)}</div>
          <div class="kpi-lbl">Saldo Intercompany</div>
          <div class="kpi-sub">${ic.tipo === 'por pagar' ? '⚠️ Por pagar' : '✓ A favor'}</div>
        </div>
        ${ic.dist_anual ? `<div class="kpi blue" style="min-width:160px">
          <div class="kpi-val">${fmtQ(ic.dist_anual)}</div>
          <div class="kpi-lbl">Por año</div>
          <div class="kpi-sub">Distribuido en ${ic.anos_distribucion} años</div>
        </div>` : ''}
      </div>`;
  }
}

function toggleAmort(id) {
  document.getElementById(id)?.classList.toggle('open');
}

// ─────────────────────────────────────────────
//  4. TIERRA Y DIVIDENDOS
// ─────────────────────────────────────────────
async function cargarTierra() {
  try {
    const d = await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}/tierra-dividendos`);
    S.tierraDividendos = d;
    renderTierra(d);
  } catch (e) {
    toast('Error cargando tierra/dividendos: ' + e.message, 'red');
  }
}

function renderTierra(d) {
  const renderPlan = (data, titulo, seccion) => {
    if (!data.plan_total) return `<div style="color:var(--muted);font-size:12px;padding:8px">Sin plan de ${titulo.toLowerCase()} para este proyecto.</div>`;
    return `
      <div class="tierra-header">
        <div class="kpi-row" style="margin-bottom:12px">
          <div class="kpi"><div class="kpi-val">${fmtQ(data.plan_total)}</div><div class="kpi-lbl">Plan total</div></div>
          <div class="kpi green"><div class="kpi-val">${fmtQ(data.pagado)}</div><div class="kpi-lbl">Pagado</div><div class="kpi-sub">${fmtPct(data.avance_pct)} avance</div></div>
          <div class="kpi red"><div class="kpi-val">${fmtQ(data.pendiente)}</div><div class="kpi-lbl">Pendiente</div></div>
        </div>
        <div style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:8px">Próximos pagos</div>
        <div style="max-height:220px;overflow-y:auto;border:1px solid var(--borde);border-radius:6px">
          <table style="width:100%;border-collapse:collapse;font-size:12px">
            <thead><tr style="background:var(--azul);color:#fff">
              <th style="padding:7px 10px;text-align:left">Fecha</th>
              <th style="padding:7px 10px;text-align:left">${seccion === 'tierra' ? 'Concepto' : 'Cuenta'}</th>
              <th style="padding:7px 10px;text-align:right">Monto</th>
            </tr></thead>
            <tbody>
              ${(data.proximos_pagos || []).map(p => `<tr style="border-bottom:1px solid var(--borde-lt)">
                <td style="padding:7px 10px">${p.fecha}</td>
                <td style="padding:7px 10px">${esc(p.concepto || p.cuenta || '—')}</td>
                <td style="padding:7px 10px;text-align:right;font-weight:600">${fmtQ(p.monto)}</td>
              </tr>`).join('') || '<tr><td colspan="3" style="padding:12px;text-align:center;color:var(--muted)">Sin pagos pendientes</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>`;
  };

  const tierraEl = document.getElementById('tierraContainer');
  const divEl    = document.getElementById('dividendosContainer');
  if (tierraEl) tierraEl.innerHTML = renderPlan(d.tierra, 'Tierra', 'tierra');
  if (divEl)    divEl.innerHTML    = renderPlan(d.dividendos, 'Dividendos', 'dividendos');
}

// ─────────────────────────────────────────────
//  FLUJO CONSOLIDADO
// ─────────────────────────────────────────────
async function cargarFlujo() {
  if (!S.empresa) return;
  syncPlazosDOM();
  try {
    const d = await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}/flujo`);
    S.flujo = d;
    renderFlujo(d);
    toast('✓ Flujo calculado', 'green');
  } catch (e) {
    toast(e.message, 'amber');
  }
}

function renderFlujo(d) {
  const ri = d.resumen_ingresos || {};
  setText('rsFlujoIngReal',   fmtQ(ri.real));
  setText('rsFlujoIngProy',   fmtQ(ri.proyectado));
  setText('rsFlujoIngTotal',  fmtQ(ri.total));
  setText('rsFlujoEgrOp',    `-${fmtQ(d.resumen_egresos_op?.pendiente)}`);
  setText('rsFlujoIVA',      `-${fmtQ(d.iva?.neto)}`);
  setText('rsFlujoFinanc',   `-${fmtQ(d.egresos_financieros?.total)}`);
  setText('rsFlujoISR',      `-${fmtQ(d.isr?.total)}`);
  setText('rsFlujoTierra',   `-${fmtQ(d.tierra_dividendos?.total)}`);

  const fn = d.flujo_neto_total || 0;
  const fnEl = document.getElementById('rsFlujoNeto');
  if (fnEl) { fnEl.textContent = fmtQ(fn); fnEl.style.color = fn >= 0 ? '#4ade80' : '#f87171'; }

  // Indicadores
  const ind = d.indicadores || {};
  setText('indTIR',    ind.tir !== null && ind.tir !== undefined ? `${ind.tir}%` : 'N/D');
  setText('indVAN',    fmtQM(ind.van));
  setText('indMargen', `${(ind.margen_neto || 0).toFixed(1)}%`);
  setText('indPE',     `${fmtNum(ind.punto_equilibrio_lotes)} lotes`);

  const tir = ind.tir;
  document.getElementById('indTIR')?.closest('.ind-card')?.setAttribute('class',
    'ind-card ' + (tir > 12 ? 'green' : tir > 0 ? 'amber' : 'red'));
  document.getElementById('indVAN')?.closest('.ind-card')?.setAttribute('class',
    'ind-card ' + ((ind.van || 0) > 0 ? 'green' : 'red'));

  // Pico negativo
  const pico = ind.pico_negativo;
  const picoEl = document.getElementById('picoAlert');
  if (picoEl) {
    if (pico && pico.anio) {
      picoEl.classList.add('show');
      setText('picoTitle', `⚠️ Pico de necesidad financiera en el Año ${pico.anio}`);
      setText('picoSub', `Flujo neto: ${fmtQ(pico.flujo)} — considerar financiamiento adicional o adelantar ventas.`);
    } else {
      picoEl.classList.remove('show');
    }
  }

  // Flujo anual — tabla transpuesta según estructura del Excel:
  // Saldo Inicial, Ingresos (sub: Reales, Vtas Activas, Proyectados),
  // Egresos Op (sub: Urbanización, Administración), Egresos Fin (sub: Intercompany, Préstamo, Intereses),
  // Impuestos (sub: IVA, ISR), Terreno, Dividendos, Saldo Final
  const tbody = document.getElementById('flujoAnualTbody');
  const thead = document.getElementById('flujoAnualThead');
  const anual = d.flujo_anual || [];
  const realAnual = d.flujo_real_anual || [];
  if (!tbody) return;

  // Combinar real + proyectado en orden cronológico
  const allYears = [];
  realAnual.forEach(r => allYears.push({...r, tipo: 'real'}));
  anual.forEach(r => {
    if (!allYears.find(a => a.anio_cal === r.anio_cal)) {
      allYears.push({...r, tipo: 'proy'});
    }
  });
  allYears.sort((a,b) => (a.anio_cal||0) - (b.anio_cal||0));
  if (!allYears.length) return;

  // Calcular saldo inicial por año (acumulado del año anterior)
  // El primer año real usa su flujo_acumulado - flujo_neto como saldo inicial
  for (let i = 0; i < allYears.length; i++) {
    if (i === 0) {
      allYears[i].saldo_ini = allYears[i].flujo_acumulado - allYears[i].flujo_neto;
    } else {
      allYears[i].saldo_ini = allYears[i-1].flujo_acumulado;
    }
  }

  // Definir filas de la tabla
  const rows = [
    {key:'saldo_ini',       label:'Saldo Inicial',         level:0, bold:true, color:'var(--azul,#1e3a5f)'},
    {key:'ingresos',        label:'Ingresos',              level:0, bold:true, color:'var(--green,#16a34a)'},
    {key:'ing_real',        label:'Ingresos Reales',       level:1},
    {key:'ing_proy',        label:'Ingreso Vtas. Activas / Proy.', level:1},
    {key:'egresos_op',      label:'Egresos Operativos',    level:0, bold:true, color:'var(--red,#dc2626)', sign:'-'},
    {key:'urbanizacion',    label:'Urbanización',          level:1, sign:'-'},
    {key:'administracion',  label:'Administración',        level:1, sign:'-'},
    {key:'egresos_fin',     label:'Egresos Financieros',   level:0, bold:true, color:'var(--amber,#d97706)', sign:'-'},
    {key:'intercompany',    label:'Intercompany',          level:1, sign:'-'},
    {key:'prestamo_capital', label:'Préstamo Bancario',    level:1, sign:'-'},
    {key:'prestamo_interes', label:'Intereses Préstamo',   level:1, sign:'-'},
    {key:'iva_neto',        label:'Impuestos',             level:0, bold:true, color:'var(--muted)', sign:'-'},
    {key:'tierra',          label:'Terreno',               level:0, bold:true, color:'var(--blue,#2563eb)', sign:'-'},
    {key:'dividendos',      label:'Dividendos',            level:0, bold:true, color:'var(--blue,#2563eb)', sign:'-'},
    {key:'flujo_neto',      label:'Flujo Neto',            level:0, bold:true, special:true},
    {key:'flujo_acumulado', label:'Saldo Final',           level:0, bold:true, special:true},
  ];

  // Header
  if (thead) {
    thead.innerHTML = `<tr style="background:var(--azul,#1e3a5f);color:#fff">
      <th style="padding:8px 12px;text-align:left;min-width:180px;position:sticky;left:0;background:var(--azul,#1e3a5f);z-index:2">Concepto</th>
      ${allYears.map(r => `<th style="padding:8px 8px;text-align:right;min-width:100px;font-size:11px">
        ${r.anio_cal}${r.tipo==='real'?' <small style="font-size:9px;opacity:.7">Real</small>':''}
      </th>`).join('')}
      <th style="padding:8px 10px;text-align:right;min-width:110px;font-weight:800;background:var(--azul,#1e3a5f)">TOTAL</th>
    </tr>`;
  }

  // Body
  const fmtCell = (val, sign) => {
    if (Math.abs(val) < 0.5) return '<span style="color:#9ca3af">—</span>';
    if (sign === '-' && val > 0) return `(${fmtQ(val)})`;
    return fmtQ(val);
  };

  tbody.innerHTML = rows.map(r => {
    const isSpecial = r.special;
    const isSection = r.level === 0 && r.bold;
    const isSub = r.level === 1;
    const bgStyle = isSpecial ? 'background:var(--azul,#1e3a5f);color:#fff;' :
                    isSection ? 'background:rgba(0,0,0,.03);' : '';
    const labelStyle = isSub ? 'padding-left:28px;font-size:11px;color:var(--muted)' :
                       `font-weight:${r.bold?700:400};color:${r.color||'inherit'};font-size:12px`;
    const total = r.key === 'flujo_acumulado' || r.key === 'saldo_ini'
      ? ''
      : allYears.reduce((s, yr) => s + (yr[r.key] || 0), 0);

    return `<tr style="${bgStyle}">
      <td style="padding:5px 12px;${labelStyle};position:sticky;left:0;z-index:1;${isSpecial?'background:var(--azul,#1e3a5f);color:#fff;':'background:var(--bg,#fff);'}">${r.label}</td>
      ${allYears.map(yr => {
        const val = yr[r.key] || 0;
        const neg = val < 0;
        const cellColor = isSpecial ? (neg ? '#f87171' : '#4ade80') : '';
        return `<td style="padding:5px 8px;text-align:right;font-size:${isSub?'10px':'11px'};${cellColor?'color:'+cellColor:''}${r.bold&&!isSub?';font-weight:600':''}">${fmtCell(val, r.sign)}</td>`;
      }).join('')}
      <td style="padding:5px 10px;text-align:right;font-weight:700;font-size:12px;${isSpecial?'background:var(--azul,#1e3a5f);color:'+(total<0?'#f87171':'#4ade80'):''}">${total !== '' ? fmtCell(total, r.sign) : ''}</td>
    </tr>`;
  }).join('');
}

function resetUI() {
  ['ingRealSaldo','ingRealContratos','ingRealCapital','ingRealInteres',
   'ingDisponibles','ingPrecioPromedio','totalIngProy',
   'rsFlujoIngReal','rsFlujoIngProy','rsFlujoIngTotal',
   'rsFlujoEgrOp','rsFlujoIVA','rsFlujoFinanc','rsFlujoISR','rsFlujoTierra','rsFlujoNeto',
   'indTIR','indVAN','indMargen','indPE'].forEach(id => setText(id, '—'));
}

// ─────────────────────────────────────────────
//  INIT
// ─────────────────────────────────────────────

async function cargarHorizonte() {
  try {
    const d = await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}/horizonte`);
    S.horizonteCalc = d.horizonte_calculado;
    S.ultimoAnioOV  = d.ultimo_anio_ov;
    const inpAnos = document.getElementById('inpAnos');
    const horizActual = parseInt(inpAnos.value) || 0;
    if (!horizActual || horizActual === 5) {
      inpAnos.value = d.horizonte_calculado;
    }
    verificarHorizonte();
  } catch (e) { /* silencioso */ }
}

function verificarHorizonte() {
  const horizActual = parseInt(document.getElementById('inpAnos').value) || 0;
  const alertEl = document.getElementById('alertaHorizonte');
  if (!alertEl) return;
  if (S.horizonteCalc && horizActual < S.horizonteCalc) {
    alertEl.style.display = 'flex';
    document.getElementById('alertaHorizonteTxt').textContent =
      `Horizonte definido (${horizActual} años) menor al calculado por OV (${S.horizonteCalc} años, hasta ${S.ultimoAnioOV}). Ingresos más allá de ${horizActual} años no se mostrarán.`;
  } else {
    if (alertEl) alertEl.style.display = 'none';
  }
}


async function limpiarProyecto() {
  if (!S.empresa) return;
  if (!confirm(`¿Seguro que querés limpiar todos los datos guardados de "${S.empresa}"? Esta acción no se puede deshacer.`)) return;
  try {
    await apiFetch(`/api/proyecciones/${encodeURIComponent(S.empresa)}`, { method: 'DELETE' });
    // Limpiar campos en pantalla
    document.getElementById('inpAnos').value = '';
    document.getElementById('inpDesc').value = '';
    document.getElementById('inpISR').value  = '';
    S.plazosVenta = [];
    S.anos = 5; S.tasaDesc = 0.12; S.pctISR = 0;
    S.horizonteCalc = null;
    renderPlazosVenta();
    resetUI();
    document.getElementById('savedBadge').style.display = 'none';
    document.getElementById('projMeta').textContent = `Empresa: ${S.empresa}`;
    document.getElementById('egresosOpContainer').innerHTML = '<div style="color:var(--muted);font-size:12px">Datos limpiados. Actualizá para ver el presupuesto vs ejecutado.</div>';
    // Recargar horizonte automático
    await cargarHorizonte();
    toast('✓ Datos del proyecto limpiados', 'green');
  } catch (e) {
    toast('Error limpiando: ' + e.message, 'red');
  }
}

document.addEventListener('DOMContentLoaded', initProyectos);
