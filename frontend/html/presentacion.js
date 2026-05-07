/* ═══════════════════════════════════════════════════════════════
   RV4 — Presentación Junta Directiva Lotificadoras
   Conectada a la API del tablero (mismo token de localStorage)
   ═══════════════════════════════════════════════════════════════ */

const API = '';   // mismo origen que el tablero
const MESES = ['','Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];

/* ── Estado global ──────────────────────────────── */
const state = {
  slide: 0,
  total: 26,
  mes: 0,        // 0 = todo el año
  anio: 2026,
  sociedad: 'EFICIENCIA URBANA',
  data: {}       // cache por endpoint
};

/* ── Auth: usa el token del tablero ─────────────── */
function getToken() {
  return localStorage.getItem('token');
}

async function apiFetch(path) {
  const token = getToken();
  if (!token) {
    setStatus('error', 'Sin sesión activa — abrir desde el tablero');
    return null;
  }
  try {
    const r = await fetch(API + path, { headers: { 'Authorization': `Bearer ${token}` } });
    if (r.status === 401) {
      setStatus('error', 'Sesión expirada');
      return null;
    }
    if (!r.ok) {
      console.warn('API error', path, r.status);
      return null;
    }
    return r.json();
  } catch (e) {
    console.error('Fetch error:', path, e);
    return null;
  }
}

/* ── Helpers de formato ─────────────────────────── */
const fmtNum = n => Number(n || 0).toLocaleString('es-GT');
const fmtQ   = n => 'Q ' + Number(n || 0).toLocaleString('es-GT', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
const fmtQM  = n => {
  const v = Number(n || 0);
  if (Math.abs(v) >= 1e6) return 'Q ' + (v/1e6).toFixed(1) + 'M';
  if (Math.abs(v) >= 1e3) return 'Q ' + (v/1e3).toFixed(0) + 'K';
  return 'Q ' + v.toFixed(0);
};
const fmtPct = n => Number(n || 0).toFixed(1) + '%';

function periodoTexto() {
  if (state.mes === 0) return `${state.anio}`;
  return `${MESES[state.mes]} ${state.anio}`;
}

function periodoFormal() {
  if (state.mes === 0) return `Año ${state.anio}`;
  return `${MESES[state.mes]} de ${state.anio}`;
}

/* ── Status indicator ───────────────────────────── */
function setStatus(kind, text) {
  const dot = document.getElementById('statusDot');
  const txt = document.getElementById('statusText');
  dot.className = 'status-dot' + (kind === 'loading' ? ' loading' : kind === 'error' ? ' error' : '');
  txt.textContent = text;
}

/* ── Tema claro/oscuro ──────────────────────────── */
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('rv4_pres_theme', theme);
  const lbl = document.getElementById('themeLbl');
  if (lbl) lbl.textContent = theme === 'dark' ? 'Claro' : 'Oscuro';
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute('data-theme') || 'light';
  applyTheme(cur === 'dark' ? 'light' : 'dark');
}

/* ── Download (PPTX / PDF via backend) ─────────── */
function toggleDownloadMenu(e) {
  e.stopPropagation();
  const m = document.getElementById('downloadMenu');
  m.classList.toggle('open');
}
document.addEventListener('click', () => {
  document.getElementById('downloadMenu')?.classList.remove('open');
});

function showDlToast(msg, err) {
  const t = document.getElementById('dlToast');
  if (!t) return;
  t.className = 'dl-toast show' + (err ? ' err' : '');
  t.innerHTML = (err ? '⚠️ ' : '⏳ ') + msg;
  clearTimeout(t._t);
  t._t = setTimeout(() => t.className = 'dl-toast', err ? 8000 : 60000);
}
function hideDlToast() {
  const t = document.getElementById('dlToast');
  if (t) t.className = 'dl-toast';
}

async function descargarPresentacion(formato) {
  document.getElementById('downloadMenu')?.classList.remove('open');
  const token = getToken();
  if (!token) { showDlToast('Sin sesión activa', true); return; }
  showDlToast('Generando presentación… puede tardar 30-60 seg.');

  try {
    const url = `/api/reportes/presentacion-consolidada?mes=${state.mes || new Date().getMonth()+1}&anio=${state.anio}&formato=${formato}`;
    const r = await fetch(url, {
      headers: { 'Authorization': `Bearer ${token}` },
      signal: AbortSignal.timeout(120000)
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({ detail: 'Error desconocido' }));
      showDlToast('Error: ' + (err.detail || r.status), true);
      return;
    }
    const blob = await r.blob();
    const burl = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const cd = r.headers.get('Content-Disposition') || '';
    const fname = cd.match(/filename="([^"]+)"/)?.[1] || `Presentacion_JD_${state.anio}_${state.mes}.${formato}`;
    a.href = burl; a.download = fname;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(burl);
    hideDlToast();
  } catch(e) {
    if (e.name === 'TimeoutError' || e.name === 'AbortError') {
      showDlToast('Timeout — la presentación sigue generándose, intenta en 1 min.', true);
    } else {
      showDlToast('Error: ' + e.message, true);
    }
  }
}

/* ── Navegación ─────────────────────────────────── */
function showSlide(idx) {
  const slides = document.querySelectorAll('.slide');
  state.total = slides.length;
  if (idx < 0) idx = 0;
  if (idx >= slides.length) idx = slides.length - 1;
  state.slide = idx;
  slides.forEach((s, i) => s.classList.toggle('active', i === idx));
  document.getElementById('tbCount').textContent = `${idx + 1} / ${slides.length}`;
  // post slide change for speaker notes
  try { window.parent.postMessage({ slideIndexChanged: idx }, '*'); } catch(e) {}
}
function nextSlide() { showSlide(state.slide + 1); }
function prevSlide() { showSlide(state.slide - 1); }

document.addEventListener('keydown', e => {
  if (['ArrowRight', 'PageDown', ' '].includes(e.key)) { e.preventDefault(); nextSlide(); }
  if (['ArrowLeft', 'PageUp'].includes(e.key))  { e.preventDefault(); prevSlide(); }
  if (e.key === 'Home') showSlide(0);
  if (e.key === 'End')  showSlide(state.total - 1);
});

/* ── Scaling del stage ──────────────────────────── */
function fitStage() {
  const stage = document.getElementById('stage');
  const deck  = document.getElementById('deck');
  const sw = stage.clientWidth, sh = stage.clientHeight;
  const dw = 1920, dh = 1080;
  const scale = Math.min(sw / dw, sh / dh) * 0.96;
  deck.style.transform = `scale(${scale})`;
}
window.addEventListener('resize', fitStage);

/* ══════════════════════════════════════════════════ */
/*  CARGADORES POR SECCIÓN                             */
/* ══════════════════════════════════════════════════ */

function periodParams() {
  const p = new URLSearchParams();
  p.set('año', state.anio);
  if (state.mes > 0) p.set('mes', state.mes);
  return p.toString();
}

// Build period params for cartera (uses año + mes directly)
function carteraPeriodParams() {
  const p = new URLSearchParams();
  if (state.mes > 0) {
    p.set('año', state.anio);
    p.set('mes', state.mes);
  }
  return p.toString();
}

/* ── Inventario ─────────────────────────────────── */
async function loadInventario() {
  const qs = periodParams();
  const r = await apiFetch(`/api/inventario/resumen?${qs}`);
  if (!r) return;
  state.data.inventario = r;
  const t = r.totales || {};
  const proyectos = r.proyectos || [];

  // Slide 5 — KPIs
  setText('invTotal', fmtNum(t.total_lotes));
  setText('invProyectos', `${proyectos.length} proyectos activos`);
  setText('invDisp', fmtNum(t.disponibles));
  setText('invDispVal', fmtQ(t.valor_disponible));
  setText('invVend', fmtNum(t.vendidos));
  setText('invVendVal', fmtQ(t.valor_vendido));
  setText('invBloq', fmtNum(t.bloqueados));
  setText('invCanjes', `${fmtNum(t.canjes || 0)} en canje`);
  setText('invAbs', fmtPct(t.pct_absorcion));
  setText('invValTotal', `${fmtQ(t.valor_total)} valor total`);

  // Donut chart
  const totalLotes = Number(t.total_lotes || 0);
  const segs = [
    { v: t.disponibles, color: 'var(--green)', name: 'Disponibles' },
    { v: t.vendidos,    color: 'var(--blue)',  name: 'Vendidos / Reservados' },
    { v: t.bloqueados,  color: 'var(--red)',   name: 'Bloqueados' },
    { v: t.canjes,      color: 'var(--purple)',name: 'Canjes' }
  ];
  drawDonut('invDonut', segs, totalLotes);
  drawLegend('invLegend', segs, totalLotes);

  // Value bars
  const valHTML = `
    <div style="display:flex;flex-direction:column;gap:14px;margin-top:8px">
      <div>
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Vendido / comprometido</span><span style="color:var(--blue)">${fmtQ(t.valor_vendido)}</span></div>
        <div style="height:16px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--blue);width:${pctOf(t.valor_vendido, t.valor_total)}%"></div></div>
      </div>
      <div>
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Disponible</span><span style="color:var(--green)">${fmtQ(t.valor_disponible)}</span></div>
        <div style="height:16px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--green);width:${pctOf(t.valor_disponible, t.valor_total)}%"></div></div>
      </div>
      <div>
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Canjes</span><span style="color:var(--purple)">${fmtQ(t.valor_canjes || 0)}</span></div>
        <div style="height:16px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--purple);width:${pctOf(t.valor_canjes || 0, t.valor_total)}%"></div></div>
      </div>
      <div>
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Bloqueados</span><span style="color:var(--red)">${fmtQ(t.valor_bloqueado || 0)}</span></div>
        <div style="height:16px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--red);width:${pctOf(t.valor_bloqueado || 0, t.valor_total)}%"></div></div>
      </div>
      <div style="display:flex;justify-content:space-between;padding-top:12px;border-top:1px solid var(--border);font-size:14px;font-weight:700"><span>Valor total inventario</span><span>${fmtQ(t.valor_total)}</span></div>
    </div>`;
  setHTML('invValueBars', valHTML);

  // Lectura
  const lectura = `Tenemos ${fmtNum(t.disponibles)} lotes disponibles por ${fmtQM(t.valor_disponible)} en ${proyectos.length} proyectos. La absorción global está en ${fmtPct(t.pct_absorcion)}.`;
  setText('invLectura', lectura);

  // Slide 6 — Absorción por proyecto (solo proyectos con movimiento)
  const conMovimiento = proyectos.filter(p => Number(p.vendidos_reservados || p.vendidos || 0) > 0);
  const sorted = [...conMovimiento].sort((a,b) => Number(b.porcentaje_absorcion||0) - Number(a.porcentaje_absorcion||0));
  const itemH = sorted.length > 8 ? 52 : 64;
  setHTML('absorcionRows', sorted.map(p => {
    const pct = Number(p.porcentaje_absorcion || 0);
    const vendidos = Number(p.vendidos_reservados || p.vendidos || 0);
    const total = Number(p.total_lotes || 0);
    return `<div class="bar-row" style="padding:${sorted.length > 8 ? '10px 0' : '14px 0'}">
      <div class="bar-name" style="font-size:${sorted.length > 8 ? '12px' : '14px'}">${p.nombre_proyecto}<div style="font-size:10px;color:var(--muted);font-weight:500;margin-top:1px">${p.nombre_sociedad}</div></div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.min(pct,100)}%"></div></div>
      <div class="bar-pct" style="font-size:13px">${fmtPct(pct)}<div style="font-size:10px;color:var(--muted);font-weight:500;margin-top:1px;text-align:right">${vendidos}/${total}</div></div>
    </div>`;
  }).join(''));

  // Slide 7 — Valor por proyecto (con ticket promedio)
  const byValor = [...proyectos].sort((a,b) => Number(b.valor_disponible||0) - Number(a.valor_disponible||0));
  setHTML('valorTbody', byValor.map((p, i) => {
    const pct = Number(p.porcentaje_absorcion || 0);
    const cls = pct >= 60 ? 'green' : pct >= 30 ? 'amber' : 'red';
    const disp = Number(p.disponibles || 0);
    const valDisp = Number(p.valor_disponible || 0);
    const ticket = disp > 0 ? valDisp / disp : 0;
    return `<tr>
      <td class="bold">${i+1}</td>
      <td class="bold">${p.nombre_proyecto}</td>
      <td>${p.nombre_sociedad}</td>
      <td class="right">${fmtNum(disp)}</td>
      <td class="right bold">${fmtQ(valDisp)}</td>
      <td class="right" style="color:var(--dorado);font-weight:700">${fmtQ(ticket)}</td>
      <td class="right"><span class="pill ${cls}">${fmtPct(pct)}</span></td>
    </tr>`;
  }).join(''));
}

/* ── Ventas ─────────────────────────────────────── */
async function loadVentas() {
  const qs = periodParams();
  const [k, mezcla, fin, vend, metas, tend] = await Promise.all([
    apiFetch(`/api/ventas/kpis?${qs}`),
    apiFetch(`/api/ventas/mezcla-financiera?${state.mes ? '' : 'todo_el_tiempo=false&'}año=${state.anio}&meses_atras=12`),
    apiFetch(`/api/ventas/analisis-financiero?${qs}`),
    apiFetch(`/api/ventas/por-vendedor?${qs}`),
    apiFetch(`/api/ventas/metas?año=${state.anio}`),
    apiFetch(`/api/ventas/tendencia-mensual?meses_atras=12${state.mes > 0 ? '&año='+state.anio+'&mes='+state.mes : ''}`)
  ]);
  state.data.ventas = { k, mezcla, fin, vend, metas, tend };

  // Slide 9 — KPIs
  if (k) {
    setText('vtBrutas', fmtNum(k.ventas_brutas));
    setText('vtValorBruto', fmtQ(k.valor_bruto));
    setText('vtDesist', fmtNum(k.desistimientos));
    setText('vtDesistVal', fmtQ(k.valor_desistido));
    setText('vtNetas', fmtNum(k.ventas_netas));
    setText('vtTasaDes', `Tasa ${fmtPct(k.tasa_desistimiento)}`);
    setText('vtTicket', fmtQ(k.ticket_promedio));
    setText('vtPlazo', `Plazo prom. ${Number(k.plazo_promedio||0).toFixed(0)} meses`);
    const subtxt = state.mes === 0
      ? `Año ${state.anio} · ${k.ventas_brutas} ventas brutas · ${k.desistimientos} desistimientos`
      : `${MESES[state.mes]} ${state.anio} · ${k.ventas_brutas} ventas brutas`;
    setText('ventasSub', subtxt);

    // Mezcla forma de pago
    const total = (k.contado||0) + (k.sin_interes||0) + (k.con_interes||0);
    const segs = [
      { v: k.contado || 0,     color: 'var(--green)',  name: 'Contado' },
      { v: k.sin_interes || 0, color: 'var(--blue)',   name: 'Crédito sin interés' },
      { v: k.con_interes || 0, color: 'var(--dorado)', name: 'Crédito con interés' }
    ];
    setHTML('vtMezcla', segs.map(s => {
      const p = total ? (s.v/total*100) : 0;
      return `<div style="margin-bottom:14px">
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span><span style="display:inline-block;width:10px;height:10px;background:${s.color};border-radius:2px;margin-right:8px"></span>${s.name}</span><span>${fmtNum(s.v)} · ${p.toFixed(1)}%</span></div>
        <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:${s.color};width:${p}%"></div></div>
      </div>`;
    }).join(''));
  }

  // Slide 9 — Composición financiera
  if (fin) {
    const cap_total = (fin.capital_contado||0) + (fin.capital_sin_int||0) + (fin.capital_con_int||0);
    const html = `
      <div style="display:flex;flex-direction:column;gap:12px">
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:700;padding-bottom:8px;border-bottom:1px solid var(--border-soft)"><span>Capital total por cobrar</span><span>${fmtQ(cap_total)}</span></div>
        <div><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;color:var(--azul);margin-bottom:4px"><span>Capital contado</span><span>${fmtQ(fin.capital_contado||0)}</span></div>
          <div style="height:10px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--azul);width:${pctOf(fin.capital_contado||0, cap_total)}%"></div></div>
        </div>
        <div><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;color:var(--green);margin-bottom:4px"><span>Intereses x cobrar</span><span>${fmtQ(fin.intereses_cobrados)}</span></div>
          <div style="height:10px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--green);width:${pctOf(fin.intereses_cobrados, fin.intereses_cobrados+fin.intereses_no_cobrados)}%"></div></div>
        </div>
        <div><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;color:var(--red);margin-bottom:4px"><span>Intereses sin cobrar (oportunidad)</span><span>${fmtQ(fin.intereses_no_cobrados)}</span></div>
          <div style="height:10px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--red);width:${pctOf(fin.intereses_no_cobrados, fin.intereses_cobrados+fin.intereses_no_cobrados)}%"></div></div>
        </div>
        <div style="padding-top:8px;border-top:1px solid var(--border);font-size:12px;color:var(--muted);font-weight:500">Tasa anual implícita: <strong style="color:var(--dorado)">${fmtPct(fin.tasa_anual_implicita)}</strong> · Captura: <strong>${fmtPct(fin.ratio_cobrado_vs_oportunidad)}</strong></div>
      </div>`;
    setHTML('vtFinanciero', html);

    // Slide 11
    setText('finTasa', fmtPct(fin.tasa_anual_implicita));
    setText('finCobrados', fmtQ(fin.intereses_cobrados));  // label updated in HTML to 'Intereses x Cobrar'
    setText('finCobLotes', `${fmtNum(fin.lotes_con_int)} contratos con interés`);
    setText('finNoCobrados', fmtQ(fin.intereses_no_cobrados));
    setText('finNoCobLotes', `${fmtNum(fin.lotes_sin_int)} contratos sin interés`);

    const lect = `La tasa implícita anual de los contratos con interés es ${fmtPct(fin.tasa_anual_implicita)}. De aplicarla a los contratos sin interés, generaría ${fmtQM(fin.intereses_no_cobrados)} adicionales — la captura actual es del ${fmtPct(fin.ratio_cobrado_vs_oportunidad)}.`;
    setText('finLectura', lect);

    setHTML('finBreakdown', `
      <tr><td class="bold">Contado</td><td class="right">${fmtNum(fin.lotes_contado)}</td><td class="right">${fmtQ(fin.capital_contado)}</td><td class="right">—</td><td class="right">—</td></tr>
      <tr><td class="bold">Crédito sin interés</td><td class="right">${fmtNum(fin.lotes_sin_int)}</td><td class="right">${fmtQ(fin.capital_sin_int)}</td><td class="right" style="color:var(--muted)">${fmtQ(fin.intereses_sin_int_pagados)}</td><td class="right">${Number(fin.plazo_prom_sin_int||0).toFixed(0)} m</td></tr>
      <tr><td class="bold">Crédito con interés</td><td class="right">${fmtNum(fin.lotes_con_int)}</td><td class="right">${fmtQ(fin.capital_con_int)}</td><td class="right" style="color:var(--green);font-weight:700">${fmtQ(fin.intereses_cobrados)}</td><td class="right">${Number(fin.plazo_prom_con_int||0).toFixed(0)} m</td></tr>
    `);
  }

  // Slide 12 — Top vendedores (mensual) + team totals
  if (vend && vend.length) {
    const top = vend.slice(0, 10);
    setHTML('vendedoresTbody', top.map((v, i) => `
      <tr>
        <td class="bold">${i+1}</td>
        <td class="bold">${v.vendedor}</td>
        <td><span class="pill ${v.equipo === 'CONSERSA' ? 'blue' : v.equipo === 'RV4' ? 'amber' : ''}">${v.equipo}</span></td>
        <td>${v.proyecto}</td>
        <td class="right">${fmtNum(v.ventas_brutas)}</td>
        <td class="right" style="color:var(--red)">${fmtNum(v.desistimientos || 0)}</td>
        <td class="right bold" style="color:var(--green)">${fmtNum(v.ventas_netas)}</td>
        <td class="right">${fmtQ(v.ticket_promedio)}</td>
      </tr>`).join(''));
    // Team totals footer
    const equipos = {};
    vend.forEach(v => {
      const eq = v.equipo || 'Sin equipo';
      if (!equipos[eq]) equipos[eq] = { brutas:0, desist:0, netas:0, totalVal:0, count:0 };
      equipos[eq].brutas += v.ventas_brutas || 0;
      equipos[eq].desist += v.desistimientos || 0;
      equipos[eq].netas  += v.ventas_netas   || 0;
      equipos[eq].totalVal += (v.ventas_netas||0) * (v.ticket_promedio||0);
      equipos[eq].count  += v.ventas_netas   || 0;
    });
    const totalGeneral = Object.values(equipos).reduce((s,e)=>s+e.netas,0);
    setHTML('vendedoresTfoot', Object.entries(equipos).map(([eq, e]) => {
      const pct = totalGeneral ? (e.netas/totalGeneral*100).toFixed(1) : '0.0';
      const cls = eq === 'CONSERSA' ? 'blue' : eq === 'RV4' ? 'amber' : '';
      const tick = e.count > 0 ? e.totalVal/e.count : 0;
      return `<tr style="border-top:2px solid var(--border)">
        <td colspan="2" class="bold" style="font-size:14px"><span class="pill ${cls}">${eq}</span></td>
        <td colspan="2" style="font-size:13px;color:var(--muted)">Participación: <strong style="color:var(--text)">${pct}%</strong></td>
        <td class="right bold">${fmtNum(e.brutas)}</td>
        <td class="right" style="color:var(--red)">${fmtNum(e.desist)}</td>
        <td class="right bold" style="color:var(--green)">${fmtNum(e.netas)}</td>
        <td class="right">${fmtQ(tick)}</td>
      </tr>`;
    }).join('') || '');
  } else {
    setHTML('vendedoresTbody', `<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:40px">Sin datos de vendedores en el período</td></tr>`);
  }

  // Slide 13 — Metas
  if (metas && metas.length) {
    setHTML('metasRows', metas.map(m => {
      if ((m.meta_total||0) === 0) return '';
      const pct  = Number(m.cumplimiento_pct||0);
      const pctC = Number(m.cumplimiento_consersa_pct||0);
      const pctR = Number(m.cumplimiento_rv4_pct||0);
      const col  = pct>=80?'var(--green)':pct>=50?'var(--dorado)':'var(--red)';
      const colC = pctC>=80?'var(--green)':pctC>=50?'var(--dorado)':'var(--red)';
      const colR = pctR>=80?'var(--green)':pctR>=50?'var(--dorado)':'var(--red)';
      const vs   = m.ventas_sin_asignar||0;
      return `<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:12px 18px;margin-bottom:10px;display:grid;grid-template-columns:170px 1fr 130px;gap:14px;align-items:center">
        <div style="font-size:14px;font-weight:700">${m.proyecto}</div>
        <div>
          <div style="display:grid;grid-template-columns:70px 1fr 55px;gap:6px;align-items:center;margin-bottom:5px">
            <span style="font-size:10px;color:var(--muted);font-weight:600">CONSERSA</span>
            <div style="height:9px;background:var(--border-soft);border-radius:3px;overflow:hidden"><div style="height:100%;background:${colC};width:${Math.min(pctC,100)}%"></div></div>
            <span style="font-size:11px;font-weight:700;color:${colC};text-align:right">${m.ventas_consersa||0}/${m.meta_consersa}</span>
          </div>
          <div style="display:grid;grid-template-columns:70px 1fr 55px;gap:6px;align-items:center">
            <span style="font-size:10px;color:var(--muted);font-weight:600">RV4</span>
            <div style="height:9px;background:var(--border-soft);border-radius:3px;overflow:hidden"><div style="height:100%;background:${colR};width:${Math.min(pctR,100)}%"></div></div>
            <span style="font-size:11px;font-weight:700;color:${colR};text-align:right">${m.ventas_rv4||0}/${m.meta_rv4}</span>
          </div>
          ${vs>0?`<div style="font-size:10px;color:var(--muted);margin-top:3px">Sin asignar: ${vs}</div>`:''}
        </div>
        <div style="text-align:right">
          <div style="font-size:20px;font-weight:700;color:${col}">${fmtPct(pct)}</div>
          <div style="font-size:10px;color:var(--muted)">${m.ventas_total||0}/${m.meta_total}</div>
        </div>
      </div>`;
    }).filter(Boolean).join(''));
  } // end metas block
} // end loadVentas

async function loadDetalleFlujos() {
  const soc = document.getElementById('detalleFlujoSociedad')?.value || 'CONSOLIDADO';
  setText('detalleFlujoSub', `${soc === 'CONSOLIDADO' ? 'Todas las sociedades' : soc} · ${periodoFormal()}`);
  if (soc === 'CONSOLIDADO') {
    // Aggregate top 3 sociedades for CONSOLIDADO view (avoid 16 parallel calls)
    const TOP_SOC = ['EFICIENCIA URBANA','SER GEN CCC','OTTAVIA','ROSSIO','FRUGALEX'];
    const ingrMapC = {}, egrMapC = {};
    const results = await Promise.all(TOP_SOC.map(s =>
      apiFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(s)}&granularidad=mes`).catch(()=>null)
    ));
    results.forEach(r => {
      if (!r || !r.periodos) return;
      let tgt = r.periodos[r.periodos.length - 1];
      if (state.mes > 0) {
        const cand = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
        if (r.periodos.includes(cand)) tgt = cand;
      }
      (r.secciones || []).forEach(sec => {
        (sec.categorias || []).forEach(cat => {
          const cv = cat.montos?.[tgt];
          if (cv?.ingreso > 0) ingrMapC[`${sec.seccion}||${cat.categoria}`] = (ingrMapC[`${sec.seccion}||${cat.categoria}`]||0) + cv.ingreso;
          if (cv?.egreso  > 0) egrMapC[`${sec.seccion}||${cat.categoria}`]  = (egrMapC[`${sec.seccion}||${cat.categoria}`]||0)  + cv.egreso;
        });
      });
    });
    renderDetalleFlujosTables(ingrMapC, egrMapC);
    return;
  }
  const r = await apiFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(soc)}&granularidad=mes`);
    if (!r || !r.periodos) return;
    let target = r.periodos[r.periodos.length - 1];
    if (state.mes > 0) {
      const candidate = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
      if (r.periodos.includes(candidate)) target = candidate;
    }
    const ingrMap = {}, egrMap = {};
    (r.secciones || []).forEach(sec => {
      (sec.categorias || []).forEach(cat => {
        const cv = cat.montos?.[target];
        if (cv?.ingreso > 0) ingrMap[`${sec.seccion}||${cat.categoria}`] = (ingrMap[`${sec.seccion}||${cat.categoria}`] || 0) + (cv.ingreso || 0);
        if (cv?.egreso  > 0) egrMap[`${sec.seccion}||${cat.categoria}`]  = (egrMap[`${sec.seccion}||${cat.categoria}`]  || 0) + (cv.egreso  || 0);
      });
    });
    renderDetalleFlujosTables(ingrMap, egrMap);
}

function renderDetalleFlujosTables(ingrMap, egrMap) {
  const topN = 8;
  const ingrRows = Object.entries(ingrMap).sort((a,b)=>b[1]-a[1]).slice(0, topN);
  const egrRows  = Object.entries(egrMap).sort((a,b)=>b[1]-a[1]).slice(0, topN);
  const toRow = ([key, val]) => {
    const [sec, cat] = key.split('||');
    return `<tr><td style="font-size:11px;color:var(--muted)">${sec.replace('EGRESOS / ','')}</td><td>${cat}</td><td class="right bold">${fmtQ(val)}</td></tr>`;
  };
  setHTML('detalleIngresosTbody', ingrRows.map(toRow).join('') || '<tr><td colspan="3" style="text-align:center;color:var(--muted);padding:20px">Sin datos</td></tr>');
  setHTML('detalleEgresosTbody', egrRows.map(toRow).join('') || '<tr><td colspan="3" style="text-align:center;color:var(--muted);padding:20px">Sin datos</td></tr>');
}

async function loadFlujos() {
  const socSel = document.getElementById('flujoSociedad')?.value || state.sociedad;
  if (socSel === 'CONSOLIDADO') {
    // Aggregate KPIs from all societies
    const sociedades = ['EFICIENCIA URBANA','SER GEN CCC','ROSSIO','FRUGALEX','OTTAVIA','UTILICA','TEZZOLI','URBIVA','GARBATELLA','CAPIPOS','OVEST','CORCOLLE','LEOFRENI','GIBRALEON','TALOCCI','VILET'];
    let totalIni=0, totalIng=0, totalEgr=0, totalFin=0;
    const secMap = {};
    const results = await Promise.all(sociedades.map(s => apiFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(s)}&granularidad=mes`)));
    results.forEach(r => {
      if (!r || !r.periodos) return;
      let target = r.periodos[r.periodos.length - 1];
      if (state.mes > 0) {
        const candidate = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
        if (r.periodos.includes(candidate)) target = candidate;
      }
      totalIni += r.saldos_iniciales[target] || 0;
      totalFin += r.saldos_finales[target] || 0;
      (r.secciones || []).forEach(sec => {
        const t = sec.totales[target];
        if (!t) return;
        totalIng += t.ingreso || 0;
        totalEgr += t.egreso  || 0;
        if (!secMap[sec.seccion]) secMap[sec.seccion] = { ingreso:0, egreso:0 };
        secMap[sec.seccion].ingreso += t.ingreso || 0;
        secMap[sec.seccion].egreso  += t.egreso  || 0;
      });
    });
    const neto = totalIng - totalEgr;
    setText('flSaldoIni', fmtQM(totalIni));
    setText('flIng', fmtQM(totalIng));
    setText('flEgr', fmtQM(totalEgr));
    setText('flSaldoFin', fmtQM(totalFin));
    setText('flNeto', `Neto del período: ${neto>=0?'+':''}${fmtQM(neto)}`);
    setText('flujoSub', `CONSOLIDADO · ${periodoFormal()}`);
    const filas = Object.entries(secMap).map(([sec, t]) => ({ seccion: sec, ingreso: t.ingreso, egreso: t.egreso, neto: t.ingreso - t.egreso }));
    setHTML('flujosTbody', [
      ...filas.map(f => `<tr>
        <td class="bold">${f.seccion}</td>
        <td class="right" style="color:var(--green)">${fmtQ(f.ingreso)}</td>
        <td class="right" style="color:var(--red)">${fmtQ(f.egreso)}</td>
        <td class="right bold" style="color:${f.neto>=0?'var(--green)':'var(--red)'}">${fmtQ(f.neto)}</td>
      </tr>`),
      `<tr class="total"><td>TOTAL</td><td class="right">${fmtQ(totalIng)}</td><td class="right">${fmtQ(totalEgr)}</td><td class="right">${fmtQ(neto)}</td></tr>`
    ].join(''));
    return;
  }

  const r = await apiFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(socSel)}&granularidad=mes`);
  state.data.flujos = r;

  if (!r || !r.periodos || !r.periodos.length) {
    setText('flSaldoIni','—'); setText('flIng','—'); setText('flEgr','—'); setText('flSaldoFin','—');
    setHTML('flujosTbody', `<tr><td colspan="4" style="text-align:center;color:var(--muted);padding:40px">Sin datos de flujos para ${state.sociedad}</td></tr>`);
    setText('flujoSub', `Sin datos disponibles para ${state.sociedad}`);
    return;
  }

  // Filtra al período seleccionado o el más reciente disponible
  const periodos = r.periodos;
  let target = periodos[periodos.length - 1];
  if (state.mes > 0) {
    const candidate = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
    if (periodos.includes(candidate)) target = candidate;
  } else {
    const yearMonths = periodos.filter(p => p.startsWith(String(state.anio)));
    if (yearMonths.length) target = yearMonths[yearMonths.length - 1];
  }

  const saldoIni = r.saldos_iniciales[target] || 0;
  const saldoFin = r.saldos_finales[target] || 0;

  let totalIng = 0, totalEgr = 0;
  const filas = [];
  for (const sec of (r.secciones || [])) {
    const t = sec.totales[target];
    if (!t) continue;
    totalIng += t.ingreso || 0;
    totalEgr += t.egreso || 0;
    filas.push({ seccion: sec.seccion, ingreso: t.ingreso, egreso: t.egreso, neto: t.neto });
  }
  const neto = totalIng - totalEgr;

  setText('flSaldoIni', fmtQM(saldoIni));
  setText('flIng', fmtQM(totalIng));
  setText('flEgr', fmtQM(totalEgr));
  setText('flSaldoFin', fmtQM(saldoFin));
  setText('flNeto', `Neto del período: ${neto >= 0 ? '+' : ''}${fmtQM(neto)}`);
  setText('flujoSub', `${socSel} · período ${target}`);

  setHTML('flujosTbody', [
    ...filas.map(f => `<tr>
      <td class="bold">${f.seccion}</td>
      <td class="right" style="color:var(--green)">${fmtQ(f.ingreso)}</td>
      <td class="right" style="color:var(--red)">${fmtQ(f.egreso)}</td>
      <td class="right bold" style="color:${f.neto>=0?'var(--green)':'var(--red)'}">${fmtQ(f.neto)}</td>
    </tr>`),
    `<tr class="total"><td>TOTAL</td><td class="right">${fmtQ(totalIng)}</td><td class="right">${fmtQ(totalEgr)}</td><td class="right">${fmtQ(neto)}</td></tr>`
  ].join(''));
}

/* ── PCV ────────────────────────────────────────── */
async function loadPCV() {
  const [k, reg] = await Promise.all([
    apiFetch('/api/ventas/pcv/kpis').catch(()=>null),
    apiFetch('/api/ventas/registros-revision').catch(()=>null)
  ]);
  state.data.pcv = { k, reg };

  if (k) {
    setText('pcvTotal', fmtNum(k.total_ventas));
    setText('pcvCon', fmtNum(k.con_pcv));
    setText('pcvPct', `${fmtPct(k.pct_cumplimiento)} de cumplimiento`);
    setText('pcvSin', fmtNum(k.sin_pcv));
    setText('pcvSin2026', `${fmtNum(k.sin_pcv_2026)} en ${state.anio} · ${fmtPct(k.pct_sin_pcv_2026)}`);
    setText('pcvDias', `${Number(k.dias_prom_gestion||0).toFixed(0)}`);
    setText('pcv0', fmtNum(k.sin_pcv_0_15));
    setText('pcv15', fmtNum(k.sin_pcv_16_30));
    setText('pcv30', fmtNum(k.sin_pcv_31_90));
    setText('pcv90', fmtNum(k.sin_pcv_mas90));
  }

  if (reg) {
    setText('regRojas', fmtNum(reg.rojas));
    setText('regAmar', fmtNum(reg.amarillas));
    setText('regGris', fmtNum(reg.grises || 0));
    setText('regTotal', fmtNum(reg.total));
    const top = (reg.issues || []).slice(0, 8);
    setHTML('regTbody', top.length
      ? top.map(i => `<tr>
          <td><span class="pill ${i.nivel === 'ROJO' ? 'red' : i.nivel === 'AMARILLO' ? 'amber' : ''}">${i.nivel}</span></td>
          <td class="bold">${humanType(i.tipo)}</td>
          <td>${i.mensaje}<div style="font-size:12px;color:var(--muted);margin-top:4px">${i.detalle || ''}</div></td>
          <td style="font-size:13px;color:var(--text-soft)">${i.accion || '—'}</td>
        </tr>`).join('')
      : `<tr><td colspan="4" style="text-align:center;color:var(--muted);padding:40px">Sin inconsistencias detectadas</td></tr>`
    );
  }
}

/* ── Resumen Ejecutivo (slide 3) — usa datos cacheados ── */
function renderResumenEjecutivo() {
  const inv  = state.data.inventario?.totales || {};
  const v    = state.data.ventas?.k || {};
  const car  = state.data.cartera?.k || {};
  const pcv  = state.data.pcv?.k || {};

  setText('rsmVentasBrutas', fmtNum(v.ventas_brutas));
  setText('rsmVentasValor', fmtQM(v.valor_bruto));
  setText('rsmDesist', fmtNum(v.desistimientos));
  setText('rsmDesistTasa', `Tasa ${fmtPct(v.tasa_desistimiento)}`);
  setText('rsmVentasNetas', fmtNum(v.ventas_netas));
  setText('rsmAbsorcion', fmtPct(inv.pct_absorcion));
  setText('rsmAbsValor', `${fmtNum(inv.disponibles)} disponibles · ${fmtQM(inv.valor_disponible)}`);
  setText('rsmCartera', fmtQM(car.cartera_total));
  setText('rsmClientes', `${fmtNum(car.clientes_activos)} clientes activos`);
  setText('rsmMoraTasa', fmtPct(car.tasa_mora));
  setText('rsmMoraMonto', fmtQM(car.mora_total));
  setText('rsmCobro30', fmtQM(car.cobro_30d));
  setText('rsmSinPCV', fmtNum(pcv.sin_pcv));
  setText('rsmPCVPct', `${fmtPct(100 - (pcv.pct_cumplimiento||0))} de las ventas`);
  setText('resumenSub', `Consolidado de las ${state.data.inventario?.proyectos?.length || 13} sociedades · ${periodoFormal()}`);
}

/* ══════════════════════════════════════════════════ */
/*  CHARTS                                             */
/* ══════════════════════════════════════════════════ */
function drawDonut(svgId, segs, total) {
  const svg = document.getElementById(svgId);
  if (!svg) return;
  const cx = 160, cy = 160, r = 110, sw = 36;
  let acc = 0;
  const totalSafe = total || segs.reduce((s,x)=>s+Number(x.v||0),0) || 1;
  const arcs = segs.filter(s => s.v > 0).map(s => {
    const frac = Number(s.v) / totalSafe;
    const start = acc, end = acc + frac;
    acc = end;
    const a0 = start * 2 * Math.PI - Math.PI/2;
    const a1 = end   * 2 * Math.PI - Math.PI/2;
    const x0 = cx + r * Math.cos(a0), y0 = cy + r * Math.sin(a0);
    const x1 = cx + r * Math.cos(a1), y1 = cy + r * Math.sin(a1);
    const large = (end - start) > 0.5 ? 1 : 0;
    return `<path d="M ${x0} ${y0} A ${r} ${r} 0 ${large} 1 ${x1} ${y1}" fill="none" stroke="${s.color}" stroke-width="${sw}" stroke-linecap="butt"/>`;
  }).join('');
  svg.innerHTML = `${arcs}
    <text x="${cx}" y="${cy-6}" text-anchor="middle" font-size="38" font-weight="700" fill="currentColor" style="font-family:Montserrat">${fmtNum(total)}</text>
    <text x="${cx}" y="${cy+22}" text-anchor="middle" font-size="13" font-weight="600" fill="var(--muted)" style="font-family:Montserrat;letter-spacing:.1em">LOTES</text>`;
}

function drawLegend(elId, segs, total) {
  const el = document.getElementById(elId);
  if (!el) return;
  const totalSafe = total || segs.reduce((s,x)=>s+Number(x.v||0),0) || 1;
  el.innerHTML = segs.map(s => {
    const p = (Number(s.v) / totalSafe * 100).toFixed(1);
    return `<div class="legend-item"><div class="legend-dot" style="background:${s.color}"></div>${s.name} <strong style="color:var(--text);margin-left:4px">${p}%</strong></div>`;
  }).join('');
}

function drawTendencia(data) {
  const svg = document.getElementById('tendenciaChart');
  if (!svg) return;
  const W = 1600, H = 480, pad = { l: 60, r: 30, t: 30, b: 60 };
  const max = Math.max(...data.map(d => Number(d.ventas_brutas||0)), 1);
  const x = i => pad.l + (i * (W - pad.l - pad.r) / Math.max(data.length-1,1));
  const y = v => H - pad.b - (v / max) * (H - pad.t - pad.b);

  const linePath = (key, color) => {
    const pts = data.map((d,i) => `${i===0?'M':'L'} ${x(i)} ${y(Number(d[key]||0))}`).join(' ');
    return `<path d="${pts}" fill="none" stroke="${color}" stroke-width="3" stroke-linejoin="round"/>` +
           data.map((d,i) => `<circle cx="${x(i)}" cy="${y(Number(d[key]||0))}" r="5" fill="${color}"/>`).join('');
  };

  // y axis grid
  let grid = '';
  for (let i=0;i<=4;i++){
    const yy = pad.t + i*(H-pad.t-pad.b)/4;
    const v = max * (1 - i/4);
    grid += `<line x1="${pad.l}" y1="${yy}" x2="${W-pad.r}" y2="${yy}" stroke="var(--border-soft)" stroke-width="1"/>`;
    grid += `<text x="${pad.l-10}" y="${yy+4}" text-anchor="end" font-size="13" fill="var(--muted)" style="font-family:Montserrat">${Math.round(v)}</text>`;
  }
  // x axis labels
  const xLabels = data.map((d,i) => {
    // DATE_TRUNC returns e.g. "2026-03-01T00:00:00" = March sales
    // Parse as UTC to avoid timezone shift
    const mesStr = String(d.mes).substring(0, 7); // "2026-03"
    const [y, m] = mesStr.split('-');
    const lbl = MESES[parseInt(m)].slice(0,3) + " " + y.slice(2);
    return `<text x="${x(i)}" y="${H-pad.b+24}" text-anchor="middle" font-size="13" fill="var(--muted)" font-weight="600" style="font-family:Montserrat">${lbl}</text>`;
  }).join('');

  svg.innerHTML = grid + xLabels +
    linePath('ventas_brutas', 'var(--azul)') +
    linePath('ventas_netas',  'var(--green)') +
    data.map((d,i) => {
      const yy = y(Number(d.desistimientos||0));
      return `<rect x="${x(i)-12}" y="${yy}" width="24" height="${H-pad.b-yy}" fill="var(--red)" opacity="0.6" rx="2"/>`;
    }).join('') +
    // Quantity labels on each point
    data.map((d,i) => `
      <text x="${x(i)}" y="${y(Number(d.ventas_brutas||0))-10}" text-anchor="middle" font-size="13" font-weight="700" fill="var(--azul)" style="font-family:Montserrat">${d.ventas_brutas||0}</text>
      <text x="${x(i)}" y="${y(Number(d.ventas_netas||0))-10}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--green)" style="font-family:Montserrat">${d.ventas_netas||0}</text>
    `).join('');
}

function drawAging(data) {
  const svg = document.getElementById('agingChart');
  if (!svg) return;
  const W = 1000, H = 400, pad = { l: 100, r: 30, t: 20, b: 80 };
  const max = Math.max(...data.map(d => Number(d.monto||0)), 1);
  const bw = (W - pad.l - pad.r) / data.length;
  const colors = ['#fbbf24', '#f59e0b', '#dc2626', '#991b1b', '#7f1d1d'];

  const bars = data.map((d, i) => {
    const v = Number(d.monto || 0);
    const h = (v / max) * (H - pad.t - pad.b);
    const x = pad.l + i * bw + bw*0.15;
    const y = H - pad.b - h;
    const w = bw * 0.7;
    return `
      <rect x="${x}" y="${y}" width="${w}" height="${h}" fill="${colors[i] || '#7f1d1d'}" rx="4"/>
      <text x="${x + w/2}" y="${y - 8}" text-anchor="middle" font-size="14" font-weight="700" fill="var(--text)" style="font-family:Montserrat">${fmtQM(v)}</text>
      <text x="${x + w/2}" y="${H - pad.b + 22}" text-anchor="middle" font-size="13" font-weight="600" fill="var(--muted)" style="font-family:Montserrat">${d.rango}</text>
      <text x="${x + w/2}" y="${H - pad.b + 42}" text-anchor="middle" font-size="11" fill="var(--muted)" style="font-family:Montserrat">${fmtNum(d.cuotas)} cuotas</text>`;
  }).join('');

  // y axis
  let grid = '';
  for (let i=0;i<=4;i++){
    const yy = pad.t + i*(H-pad.t-pad.b)/4;
    const v = max * (1 - i/4);
    grid += `<line x1="${pad.l}" y1="${yy}" x2="${W-pad.r}" y2="${yy}" stroke="var(--border-soft)" stroke-width="1"/>`;
    grid += `<text x="${pad.l-10}" y="${yy+4}" text-anchor="end" font-size="11" fill="var(--muted)" style="font-family:Montserrat">${fmtQM(v)}</text>`;
  }

  svg.innerHTML = grid + bars;
}

function drawProyeccion(data) {
  const svg = document.getElementById('proyChart');
  if (!svg) return;
  const W = 1600, H = 480, pad = { l: 80, r: 30, t: 30, b: 70 };
  const max = Math.max(...data.map(d => Number(d.total||0)), 1);
  const bw = (W - pad.l - pad.r) / data.length;

  const bars = data.map((d, i) => {
    const cap = Number(d.capital || 0);
    const intr = Number(d.intereses || 0);
    const totalH = ((cap + intr) / max) * (H - pad.t - pad.b);
    const capH = (cap / max) * (H - pad.t - pad.b);
    const intH = (intr / max) * (H - pad.t - pad.b);
    const x = pad.l + i * bw + bw * 0.15;
    const w = bw * 0.7;
    const yCap = H - pad.b - capH;
    const yInt = yCap - intH;
    const mesStr = String(d.mes).substring(0,7); const [y,m] = mesStr.split('-');
    const lbl = MESES[parseInt(m)].slice(0,3);
    return `
      <rect x="${x}" y="${yInt}" width="${w}" height="${intH}" fill="var(--purple)" rx="3"/>
      <rect x="${x}" y="${yCap}" width="${w}" height="${capH}" fill="var(--blue)"/>
      <text x="${x + w/2}" y="${yInt - 6}" text-anchor="middle" font-size="11" font-weight="700" fill="var(--text)" style="font-family:Montserrat">${fmtQM(cap+intr)}</text>
      <text x="${x + w/2}" y="${H - pad.b + 22}" text-anchor="middle" font-size="13" font-weight="600" fill="var(--muted)" style="font-family:Montserrat">${lbl}</text>`;
  }).join('');

  let grid = '';
  for (let i=0;i<=4;i++){
    const yy = pad.t + i*(H-pad.t-pad.b)/4;
    const v = max * (1 - i/4);
    grid += `<line x1="${pad.l}" y1="${yy}" x2="${W-pad.r}" y2="${yy}" stroke="var(--border-soft)" stroke-width="1"/>`;
    grid += `<text x="${pad.l-10}" y="${yy+4}" text-anchor="end" font-size="12" fill="var(--muted)" style="font-family:Montserrat">${fmtQM(v)}</text>`;
  }

  svg.innerHTML = grid + bars;
}

/* ══════════════════════════════════════════════════ */
/*  DEMO MODE — datos de muestra cuando no hay token   */
/* ══════════════════════════════════════════════════ */
function loadDemoData() {
  state.data = {
    inventario: {
      proyectos: [
        { proyecto_id:1, nombre_proyecto:'Hacienda Jumay', nombre_sociedad:'Eficiencia Urbana', total_lotes:420, disponibles:180, vendidos_reservados:210, bloqueados:20, canjes:10, valor_disponible:32500000, valor_total:78000000, porcentaje_absorcion:50.0 },
        { proyecto_id:2, nombre_proyecto:'La Ceiba', nombre_sociedad:'Servicios Generales', total_lotes:285, disponibles:88, vendidos_reservados:175, bloqueados:15, canjes:7, valor_disponible:14500000, valor_total:44000000, porcentaje_absorcion:61.4 },
        { proyecto_id:3, nombre_proyecto:'Hacienda el Sol', nombre_sociedad:'Rossio', total_lotes:512, disponibles:340, vendidos_reservados:150, bloqueados:18, canjes:4, valor_disponible:48000000, valor_total:79000000, porcentaje_absorcion:29.3 },
        { proyecto_id:4, nombre_proyecto:'Oasis Zacapa', nombre_sociedad:'Frugalex', total_lotes:198, disponibles:82, vendidos_reservados:108, bloqueados:6, canjes:2, valor_disponible:11200000, valor_total:26800000, porcentaje_absorcion:54.5 },
        { proyecto_id:5, nombre_proyecto:'Cañadas de Jalapa', nombre_sociedad:'Ottavia', total_lotes:312, disponibles:160, vendidos_reservados:140, bloqueados:8, canjes:4, valor_disponible:18900000, valor_total:42500000, porcentaje_absorcion:44.9 },
        { proyecto_id:6, nombre_proyecto:'Condado Jutiapa', nombre_sociedad:'Utilica', total_lotes:240, disponibles:120, vendidos_reservados:110, bloqueados:7, canjes:3, valor_disponible:16800000, valor_total:36000000, porcentaje_absorcion:45.8 },
        { proyecto_id:7, nombre_proyecto:'Club Campestre Jumay', nombre_sociedad:'Tezzoli', total_lotes:158, disponibles:78, vendidos_reservados:72, bloqueados:5, canjes:3, valor_disponible:23400000, valor_total:48000000, porcentaje_absorcion:45.6 },
        { proyecto_id:8, nombre_proyecto:'Club del Bosque', nombre_sociedad:'Urbiva 2', total_lotes:142, disponibles:62, vendidos_reservados:75, bloqueados:3, canjes:2, valor_disponible:9800000, valor_total:22500000, porcentaje_absorcion:52.8 },
        { proyecto_id:9, nombre_proyecto:'Club Residencial Progreso', nombre_sociedad:'Garbatella', total_lotes:225, disponibles:130, vendidos_reservados:88, bloqueados:5, canjes:2, valor_disponible:18200000, valor_total:31500000, porcentaje_absorcion:39.1 },
        { proyecto_id:10,nombre_proyecto:'Arboleda Santa Elena', nombre_sociedad:'Capipos', total_lotes:178, disponibles:54, vendidos_reservados:118, bloqueados:4, canjes:2, valor_disponible:7200000, valor_total:23800000, porcentaje_absorcion:66.3 },
        { proyecto_id:11,nombre_proyecto:'Hacienda Santa Lucia', nombre_sociedad:'Ovest', total_lotes:198, disponibles:115, vendidos_reservados:78, bloqueados:3, canjes:2, valor_disponible:14000000, valor_total:24600000, porcentaje_absorcion:39.4 },
        { proyecto_id:12,nombre_proyecto:'Hacienda El Cafetal Fase I', nombre_sociedad:'Corcolle', total_lotes:265, disponibles:158, vendidos_reservados:100, bloqueados:5, canjes:2, valor_disponible:21500000, valor_total:38500000, porcentaje_absorcion:37.7 },
        { proyecto_id:13,nombre_proyecto:'Hacienda El Cafetal Fase III', nombre_sociedad:'Gibraleon', total_lotes:185, disponibles:142, vendidos_reservados:38, bloqueados:3, canjes:2, valor_disponible:18800000, valor_total:24500000, porcentaje_absorcion:20.5 }
      ],
      totales: { total_lotes:3318, disponibles:1709, vendidos:1462, bloqueados:102, canjes:45, valor_disponible:254800000, valor_vendido:212400000, valor_total:519700000, pct_absorcion:44.1 }
    },
    ventas: {
      k: { ventas_brutas:142, valor_bruto:32800000, intereses_pactados:8500000, ticket_promedio:231000, contado:18, sin_interes:64, con_interes:60, plazo_promedio:54, sin_vendedor:8, desistimientos:14, valor_desistido:3200000, ventas_netas:128, tasa_desistimiento:9.9 },
      mezcla: [],
      fin: { lotes_contado:18, lotes_sin_int:64, lotes_con_int:60, capital_contado:4200000, capital_sin_int:14600000, capital_con_int:14000000, intereses_cobrados:5800000, intereses_sin_int_pagados:0, plazo_prom_con_int:60, plazo_prom_sin_int:48, tasa_anual_implicita:8.3, tasa_total_sobre_capital:41.4, intereses_no_cobrados:4850000, ratio_cobrado_vs_oportunidad:54.5 },
      vend: [
        { vendedor:'María Alejandra Ramírez', equipo:'CONSERSA', proyecto:'Hacienda Jumay', ventas_brutas:24, valor_bruto:5800000, ticket_promedio:241000, plazo_promedio:54, contado:3, sin_interes:11, con_interes:10, desistimientos:2, ventas_netas:22 },
        { vendedor:'José Eduardo Castillo', equipo:'RV4', proyecto:'La Ceiba', ventas_brutas:21, valor_bruto:4900000, ticket_promedio:233000, plazo_promedio:48, contado:2, sin_interes:10, con_interes:9, desistimientos:1, ventas_netas:20 },
        { vendedor:'Ana Lucía Méndez', equipo:'CONSERSA', proyecto:'Cañadas de Jalapa', ventas_brutas:18, valor_bruto:4200000, ticket_promedio:233000, plazo_promedio:54, contado:2, sin_interes:8, con_interes:8, desistimientos:2, ventas_netas:16 },
        { vendedor:'Carlos Roberto Pineda', equipo:'RV4', proyecto:'Oasis Zacapa', ventas_brutas:15, valor_bruto:3500000, ticket_promedio:233000, plazo_promedio:60, contado:1, sin_interes:7, con_interes:7, desistimientos:1, ventas_netas:14 },
        { vendedor:'Lucía Fernanda López', equipo:'CONSERSA', proyecto:'Condado Jutiapa', ventas_brutas:14, valor_bruto:3260000, ticket_promedio:232000, plazo_promedio:48, contado:2, sin_interes:6, con_interes:6, desistimientos:1, ventas_netas:13 },
        { vendedor:'Roberto Jiménez', equipo:'RV4', proyecto:'Club del Bosque', ventas_brutas:12, valor_bruto:2800000, ticket_promedio:233000, plazo_promedio:54, contado:2, sin_interes:5, con_interes:5, desistimientos:1, ventas_netas:11 },
        { vendedor:'Patricia Morales', equipo:'CONSERSA', proyecto:'Arboleda Santa Elena', ventas_brutas:11, valor_bruto:2570000, ticket_promedio:233000, plazo_promedio:48, contado:1, sin_interes:5, con_interes:5, desistimientos:1, ventas_netas:10 },
        { vendedor:'Daniel Estrada', equipo:'RV4', proyecto:'Hacienda Santa Lucia', ventas_brutas:10, valor_bruto:2330000, ticket_promedio:233000, plazo_promedio:54, contado:1, sin_interes:5, con_interes:4, desistimientos:1, ventas_netas:9 },
        { vendedor:'Sofía Hernández', equipo:'CONSERSA', proyecto:'Hacienda El Cafetal F.I', ventas_brutas:9, valor_bruto:2100000, ticket_promedio:233000, plazo_promedio:48, contado:1, sin_interes:4, con_interes:4, desistimientos:1, ventas_netas:8 },
        { vendedor:'Miguel Ángel Reyes', equipo:'RV4', proyecto:'Club Residencial Progreso', ventas_brutas:8, valor_bruto:1860000, ticket_promedio:232000, plazo_promedio:54, contado:1, sin_interes:4, con_interes:3, desistimientos:1, ventas_netas:7 }
      ],
      metas: [
        { responsable:'Eduardo Méndez', proyecto:'Hacienda Jumay', meta_consersa:120, meta_rv4:80, meta_total:200, ventas_consersa:78, ventas_rv4:54, ventas_total:132, cumplimiento_pct:66.0 },
        { responsable:'Eduardo Méndez', proyecto:'La Ceiba',       meta_consersa:80,  meta_rv4:60, meta_total:140, ventas_consersa:55, ventas_rv4:42, ventas_total:97,  cumplimiento_pct:69.3 },
        { responsable:'Karla Ramírez',   proyecto:'Cañadas de Jalapa', meta_consersa:60, meta_rv4:40, meta_total:100, ventas_consersa:32, ventas_rv4:22, ventas_total:54, cumplimiento_pct:54.0 },
        { responsable:'Karla Ramírez',   proyecto:'Condado Jutiapa', meta_consersa:50, meta_rv4:30, meta_total:80, ventas_consersa:28, ventas_rv4:18, ventas_total:46, cumplimiento_pct:57.5 },
        { responsable:'Roberto Solís',   proyecto:'Hacienda el Sol', meta_consersa:90, meta_rv4:70, meta_total:160, ventas_consersa:42, ventas_rv4:34, ventas_total:76, cumplimiento_pct:47.5 },
        { responsable:'Roberto Solís',   proyecto:'Oasis Zacapa', meta_consersa:50, meta_rv4:35, meta_total:85, ventas_consersa:32, ventas_rv4:25, ventas_total:57, cumplimiento_pct:67.1 }
      ],
      tend: [
        { mes:'2025-12-01', ventas_brutas:18, valor_bruto:4100000, desistimientos:2, ventas_netas:16 },
        { mes:'2026-01-01', ventas_brutas:14, valor_bruto:3200000, desistimientos:1, ventas_netas:13 },
        { mes:'2026-02-01', ventas_brutas:16, valor_bruto:3700000, desistimientos:2, ventas_netas:14 },
        { mes:'2026-03-01', ventas_brutas:22, valor_bruto:5100000, desistimientos:1, ventas_netas:21 },
        { mes:'2026-04-01', ventas_brutas:19, valor_bruto:4400000, desistimientos:2, ventas_netas:17 },
        { mes:'2026-05-01', ventas_brutas:21, valor_bruto:4850000, desistimientos:3, ventas_netas:18 },
        { mes:'2026-06-01', ventas_brutas:17, valor_bruto:3950000, desistimientos:1, ventas_netas:16 },
        { mes:'2026-07-01', ventas_brutas:23, valor_bruto:5300000, desistimientos:2, ventas_netas:21 },
        { mes:'2026-08-01', ventas_brutas:20, valor_bruto:4620000, desistimientos:1, ventas_netas:19 },
        { mes:'2026-09-01', ventas_brutas:18, valor_bruto:4180000, desistimientos:2, ventas_netas:16 },
        { mes:'2026-10-01', ventas_brutas:22, valor_bruto:5080000, desistimientos:1, ventas_netas:21 },
        { mes:'2026-11-01', ventas_brutas:19, valor_bruto:4380000, desistimientos:2, ventas_netas:17 }
      ]
    },
    cartera: {
      k: { capital_total:148000000, intereses_total:42000000, cartera_total:190000000, mora_total:24500000, clientes_activos:1840, clientes_vencidos:185, cobro_30d:8200000, cobro_60d:15600000, cobro_90d:23800000, cobro_365d:96500000, desistimientos_total:148, desistimientos_pagado:18500000, desistimientos_reintegrado:12200000, tasa_mora:12.9 },
      aging: [
        { rango:'1-30 días',  cuotas:240, clientes:120, monto:5400000 },
        { rango:'31-60 días', cuotas:180, clientes:88,  monto:4200000 },
        { rango:'61-90 días', cuotas:140, clientes:72,  monto:3800000 },
        { rango:'91-180 días',cuotas:165, clientes:80,  monto:5600000 },
        { rango:'+180 días',  cuotas:280, clientes:115, monto:5500000 }
      ],
      proy: Array.from({length:12},(_,i)=>{
        const m = new Date(2026, i, 1);
        return { mes: m.toISOString().slice(0,10), capital: 4500000+Math.random()*2500000, intereses: 1200000+Math.random()*800000, total: 0, num_cuotas: 80+Math.floor(Math.random()*40) };
      }).map(d => ({...d, total: d.capital+d.intereses})),
      alertas: {
        total:18, rojas:11, amarillas:7, alertas:[
          { tipo:'VENCIDO_90_DIAS', nivel:'ROJO', mensaje:'Vencido +90 días: García López, Carlos', detalle:'4 cuotas | Q 285,000 | Desde: 2026-08-15' },
          { tipo:'VENCIDO_90_DIAS', nivel:'ROJO', mensaje:'Vencido +90 días: Méndez Ramírez, Patricia', detalle:'3 cuotas | Q 198,000 | Desde: 2026-09-02' },
          { tipo:'ALTA_CONCENTRACION', nivel:'AMARILLO', mensaje:'Alta concentración: Inversiones del Sol S.A. — 14.2% de Rossio', detalle:'Saldo: Q 6,850,000 de Q 48,200,000 totales' },
          { tipo:'SOBREPAGO', nivel:'ROJO', mensaje:'Saldo negativo: Hernández Pérez, María — Eficiencia Urbana', detalle:'Lote: M5-L23 | Saldo: Q -12,400.00' },
          { tipo:'DESISTIMIENTO_CON_CARTERA', nivel:'ROJO', mensaje:'Desistimiento con cartera abierta: Ramos Castillo, José', detalle:'Lote: M2-L08 | Desistió: 2026-06-12 | Saldo abierto: Q 84,500.00' },
          { tipo:'VENCIDO_90_DIAS', nivel:'ROJO', mensaje:'Vencido +90 días: López Aguilar, Roberto', detalle:'5 cuotas | Q 320,000 | Desde: 2026-07-20' },
          { tipo:'ALTA_CONCENTRACION', nivel:'AMARILLO', mensaje:'Alta concentración: Constructora del Norte — 11.8% de Frugalex', detalle:'Saldo: Q 1,320,000 de Q 11,180,000 totales' },
          { tipo:'VENCIDO_90_DIAS', nivel:'ROJO', mensaje:'Vencido +90 días: Morales Castillo, Ana', detalle:'4 cuotas | Q 252,000 | Desde: 2026-08-01' }
        ]
      },
      desist: { total:148, desistimientos:[] }
    },
    flujos: null,
    pcv: {
      k: { total_ventas:2450, con_pcv:1980, sin_pcv:470, ventas_2026:142, sin_pcv_2026:38, sin_pcv_0_15:12, sin_pcv_16_30:18, sin_pcv_31_90:85, sin_pcv_mas30:440, sin_pcv_mas90:355, dias_prom_gestion:22, pct_cumplimiento:80.8, pct_sin_pcv_2026:26.8 },
      reg: {
        total:14, rojas:4, amarillas:8, grises:2, issues:[
          { tipo:'CON_INTERES_SIN_MONTO', nivel:'ROJO', mensaje:'Crédito con interés sin monto de interés: López, María', detalle:'Hacienda Jumay | M3-L12 | Q 285,000 | Plazo: 60m | Intereses: Q 0', accion:'Revisar contrato en SAP — el campo Total Intereses está en cero' },
          { tipo:'SIN_INTERES_CON_MONTO', nivel:'ROJO', mensaje:'Crédito sin interés con monto de interés registrado: Ramos, Carlos', detalle:'La Ceiba | M1-L05 | Capital: Q 240,000 | Intereses registrados: Q 38,500', accion:'Verificar tipo de crédito en SAP' },
          { tipo:'VENDEDOR_NUEVO', nivel:'AMARILLO', mensaje:'Nuevo vendedor detectado en SAP: Estrada Fuentes, Daniel', detalle:'Fue agregado automáticamente — asignar equipo CONSERSA o RV4', accion:'Asignar equipo en configuración' },
          { tipo:'VENDEDOR_SIN_EQUIPO', nivel:'AMARILLO', mensaje:'Vendedor sin equipo asignado: Solís Aguilar, Miguel', detalle:'8 ventas registradas sin equipo', accion:'Ir a configuración de vendedores' },
          { tipo:'CREDITO_SIN_PLAZO', nivel:'AMARILLO', mensaje:'Crédito sin plazo definido: Castillo, Ana', detalle:'Cañadas de Jalapa | M2-L10 | CREDITOCONINTERES | Q 198,000', accion:'Registrar el plazo correcto en SAP' },
          { tipo:'FECHA_DESISTIMIENTO_INCORRECTA', nivel:'ROJO', mensaje:'Desistimiento antes de la venta: Pérez, José', detalle:'Lote: M4-L08 | Venta: 2026-08-15 | Desistimiento: 2026-08-10', accion:'Corregir fecha en SAP' },
          { tipo:'VENTA_SIN_VENDEDOR', nivel:'GRIS', mensaje:'Registro especial: \'Canje A\'', detalle:'12 lotes | Q 2,800,000 — no se incluyen en KPIs', accion:'Revisar si corresponde a canje' },
          { tipo:'VENDEDOR_NUEVO', nivel:'AMARILLO', mensaje:'Nuevo vendedor detectado: Hernández, Sofía', detalle:'Asignar equipo', accion:'Asignar equipo' }
        ]
      }
    }
  };
}

function renderDemoFlujos() {
  // Flujo de muestra para slide 21
  const ing = 12500000, egr = 8200000, neto = ing - egr;
  setText('flSaldoIni', fmtQM(4200000));
  setText('flIng', fmtQM(ing));
  setText('flEgr', fmtQM(egr));
  setText('flSaldoFin', fmtQM(4200000 + neto));
  setText('flNeto', `Neto del período: +${fmtQM(neto)}`);
  setText('flujoSub', `${state.sociedad} · período de muestra`);
  setHTML('flujosTbody', `
    <tr><td class="bold">INGRESOS</td><td class="right" style="color:var(--green)">${fmtQ(ing)}</td><td class="right" style="color:var(--red)">${fmtQ(0)}</td><td class="right bold" style="color:var(--green)">${fmtQ(ing)}</td></tr>
    <tr><td class="bold">EGRESOS / URBANIZACION</td><td class="right" style="color:var(--green)">${fmtQ(0)}</td><td class="right" style="color:var(--red)">${fmtQ(4500000)}</td><td class="right bold" style="color:var(--red)">${fmtQ(-4500000)}</td></tr>
    <tr><td class="bold">EGRESOS / ADMINISTRACION</td><td class="right" style="color:var(--green)">${fmtQ(0)}</td><td class="right" style="color:var(--red)">${fmtQ(2200000)}</td><td class="right bold" style="color:var(--red)">${fmtQ(-2200000)}</td></tr>
    <tr><td class="bold">FINANCIAMIENTO</td><td class="right" style="color:var(--green)">${fmtQ(0)}</td><td class="right" style="color:var(--red)">${fmtQ(800000)}</td><td class="right bold" style="color:var(--red)">${fmtQ(-800000)}</td></tr>
    <tr><td class="bold">IMPUESTOS</td><td class="right" style="color:var(--green)">${fmtQ(0)}</td><td class="right" style="color:var(--red)">${fmtQ(700000)}</td><td class="right bold" style="color:var(--red)">${fmtQ(-700000)}</td></tr>
    <tr class="total"><td>TOTAL</td><td class="right">${fmtQ(ing)}</td><td class="right">${fmtQ(egr)}</td><td class="right">${fmtQ(neto)}</td></tr>
  `);
}

/* ══════════════════════════════════════════════════ */
/*  ENTRY                                              */
/* ══════════════════════════════════════════════════ */
function setText(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
function setHTML(id, v) { const el = document.getElementById(id); if (el) el.innerHTML = v; }
function pctOf(a, b) { const av = Number(a||0), bv = Number(b||0); return bv ? (av/bv*100) : 0; }
function humanType(t) {
  if (!t) return '';
  return t.replace(/_/g,' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

function updateAllPeriodLabels() {
  document.querySelectorAll('[data-period]').forEach(el => el.textContent = `Período: ${periodoFormal()}`);
  setText('coverPeriod', `Período: ${periodoFormal()}`);
  setText('meetingDate', new Date().toLocaleDateString('es-GT', { day:'numeric', month:'long', year:'numeric' }));
  setText('coverSociedades', `${state.data.inventario?.proyectos?.length || 13} proyectos activos`);
  setText('closeDate', new Date().toLocaleString('es-GT', { day:'numeric', month:'long', year:'numeric', hour:'2-digit', minute:'2-digit' }));
  setText('ftr2', `Período: ${periodoFormal()}`);
}

async function loadAll() {
  window._loadStart = Date.now();
  setStatus('loading', 'Cargando datos… (puede tardar 5-15 segundos)');
  const token = getToken();

  if (!token) {
    // Demo mode — useful for design preview & opening file directly
    loadDemoData();
    setStatus('error', 'Modo demo · sin sesión');

    // Render with demo data
    await renderInventarioSlides();
    await renderVentasSlides();
    await renderCarteraSlides();
    renderDemoFlujos();
    // detalle flujos - skip in demo mode (no data)
    await renderPCVSlides();
    renderResumenEjecutivo();
    updateAllPeriodLabels();
    return;
  }

  // Live data
  try {
    await Promise.all([loadInventario(), loadVentas(), loadCartera(), loadFlujos(), loadDetalleFlujos(), loadPCV()]);
    renderResumenEjecutivo();
    updateAllPeriodLabels();
    const loadEnd = Date.now();
    setStatus('ok', `Datos en vivo · ${periodoFormal()}`);
    // Show load time in footer
    const loadMs = loadEnd - (window._loadStart || loadEnd);
    if (loadMs > 500) console.log(`Datos cargados en ${(loadMs/1000).toFixed(1)}s`);
  } catch (e) {
    console.error('Load error:', e);
    setStatus('error', 'Error cargando datos');
  }
}

/* Render con datos demo (mismo código que los renderers reales pero sin fetch) */
async function renderInventarioSlides() {
  const r = state.data.inventario;
  if (!r) return;
  const t = r.totales || {}, proyectos = r.proyectos || [];
  setText('invTotal', fmtNum(t.total_lotes));
  setText('invProyectos', `${proyectos.length} proyectos activos`);
  setText('invDisp', fmtNum(t.disponibles));
  setText('invDispVal', fmtQ(t.valor_disponible));
  setText('invVend', fmtNum(t.vendidos));
  setText('invVendVal', fmtQ(t.valor_vendido));
  setText('invBloq', fmtNum(t.bloqueados));
  setText('invCanjes', `${fmtNum(t.canjes||0)} en canje`);
  setText('invAbs', fmtPct(t.pct_absorcion));
  setText('invValTotal', `${fmtQ(t.valor_total)} valor total`);

  const segs = [
    { v: t.disponibles, color:'#16a34a', name:'Disponibles' },
    { v: t.vendidos,    color:'#1d4ed8', name:'Vendidos / Reservados' },
    { v: t.bloqueados,  color:'#b91c1c', name:'Bloqueados' },
    { v: t.canjes,      color:'#6d28d9', name:'Canjes' }
  ];
  drawDonut('invDonut', segs, t.total_lotes);
  drawLegend('invLegend', segs, t.total_lotes);

  const valHTML = `
    <div style="display:flex;flex-direction:column;gap:18px;margin-top:8px">
      <div>
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Vendido / comprometido</span><span style="color:var(--blue)">${fmtQ(t.valor_vendido)}</span></div>
        <div style="height:18px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--blue);width:${pctOf(t.valor_vendido,t.valor_total)}%"></div></div>
      </div>
      <div>
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Disponible</span><span style="color:var(--green)">${fmtQ(t.valor_disponible)}</span></div>
        <div style="height:18px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--green);width:${pctOf(t.valor_disponible,t.valor_total)}%"></div></div>
      </div>
      <div style="display:flex;justify-content:space-between;padding-top:12px;border-top:1px solid var(--border);font-size:14px;font-weight:700"><span>Valor total inventario</span><span>${fmtQ(t.valor_total)}</span></div>
    </div>`;
  setHTML('invValueBars', valHTML);
  setText('invLectura', `Tenemos ${fmtNum(t.disponibles)} lotes disponibles por ${fmtQM(t.valor_disponible)} en ${proyectos.length} proyectos. La absorción global está en ${fmtPct(t.pct_absorcion)}.`);

  const sorted = [...proyectos].sort((a,b)=>Number(b.porcentaje_absorcion||0)-Number(a.porcentaje_absorcion||0));
  setHTML('absorcionRows', sorted.map(p => {
    const pct = Number(p.porcentaje_absorcion||0);
    return `<div class="bar-row">
      <div class="bar-name">${p.nombre_proyecto}<div style="font-size:11px;color:var(--muted);font-weight:500;margin-top:2px">${p.nombre_sociedad}</div></div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.min(pct,100)}%"></div></div>
      <div class="bar-pct">${fmtPct(pct)}</div>
    </div>`;
  }).join(''));

  const byValor = [...proyectos].sort((a,b)=>Number(b.valor_disponible||0)-Number(a.valor_disponible||0));
  setHTML('valorTbody', byValor.map((p,i)=>{
    const pct = Number(p.porcentaje_absorcion||0);
    const cls = pct>=60?'green':pct>=30?'amber':'red';
    return `<tr>
      <td class="bold">${i+1}</td><td class="bold">${p.nombre_proyecto}</td><td>${p.nombre_sociedad}</td>
      <td class="right">${fmtNum(p.disponibles)}</td><td class="right bold">${fmtQ(p.valor_disponible)}</td>
      <td class="right"><span class="pill ${cls}">${fmtPct(pct)}</span></td>
    </tr>`;
  }).join(''));
}

async function renderVentasSlides() {
  const v = state.data.ventas;
  if (!v) return;
  const k = v.k;
  if (k) {
    setText('vtBrutas', fmtNum(k.ventas_brutas));
    setText('vtValorBruto', fmtQ(k.valor_bruto));
    setText('vtDesist', fmtNum(k.desistimientos));
    setText('vtDesistVal', fmtQ(k.valor_desistido));
    setText('vtNetas', fmtNum(k.ventas_netas));
    setText('vtTasaDes', `Tasa ${fmtPct(k.tasa_desistimiento)}`);
    setText('vtTicket', fmtQ(k.ticket_promedio));
    setText('vtPlazo', `Plazo prom. ${Number(k.plazo_promedio||0).toFixed(0)} meses`);
    setText('ventasSub', `${periodoFormal()} · ${k.ventas_brutas} ventas brutas · ${k.desistimientos} desistimientos`);

    const total = (k.contado||0)+(k.sin_interes||0)+(k.con_interes||0);
    const segs = [
      { v:k.contado||0, color:'var(--green)', name:'Contado' },
      { v:k.sin_interes||0, color:'var(--blue)', name:'Crédito sin interés' },
      { v:k.con_interes||0, color:'var(--dorado)', name:'Crédito con interés' }
    ];
    setHTML('vtMezcla', segs.map(s=>{
      const p = total?(s.v/total*100):0;
      return `<div style="margin-bottom:14px">
        <div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span><span style="display:inline-block;width:10px;height:10px;background:${s.color};border-radius:2px;margin-right:8px"></span>${s.name}</span><span>${fmtNum(s.v)} · ${p.toFixed(1)}%</span></div>
        <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:${s.color};width:${p}%"></div></div>
      </div>`;
    }).join(''));
  }

  const fin = v.fin;
  if (fin) {
    const cap_total = (fin.capital_contado||0)+(fin.capital_sin_int||0)+(fin.capital_con_int||0);
    setHTML('vtFinanciero', `
      <div style="display:flex;flex-direction:column;gap:14px">
        <div><div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;margin-bottom:6px"><span>Capital total</span><span>${fmtQ(cap_total)}</span></div></div>
        <div><div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;color:var(--green);margin-bottom:6px"><span>Intereses cobrados</span><span>${fmtQ(fin.intereses_cobrados)}</span></div>
          <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--green);width:${pctOf(fin.intereses_cobrados,fin.intereses_cobrados+fin.intereses_no_cobrados)}%"></div></div>
        </div>
        <div><div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;color:var(--red);margin-bottom:6px"><span>Intereses no cobrados</span><span>${fmtQ(fin.intereses_no_cobrados)}</span></div>
          <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--red);width:${pctOf(fin.intereses_no_cobrados,fin.intereses_cobrados+fin.intereses_no_cobrados)}%"></div></div>
        </div>
        <div style="padding-top:10px;border-top:1px solid var(--border);font-size:13px;color:var(--muted);font-weight:500">Tasa anual implícita: <strong style="color:var(--dorado)">${fmtPct(fin.tasa_anual_implicita)}</strong> · Captura: <strong>${fmtPct(fin.ratio_cobrado_vs_oportunidad)}</strong></div>
      </div>`);

    setText('finTasa', fmtPct(fin.tasa_anual_implicita));
    setText('finCobrados', fmtQ(fin.intereses_cobrados));  // label updated in HTML to 'Intereses x Cobrar'
    setText('finCobLotes', `${fmtNum(fin.lotes_con_int)} contratos con interés`);
    setText('finNoCobrados', fmtQ(fin.intereses_no_cobrados));
    setText('finNoCobLotes', `${fmtNum(fin.lotes_sin_int)} contratos sin interés`);
    setText('finLectura', `La tasa implícita anual de los contratos con interés es ${fmtPct(fin.tasa_anual_implicita)}. De aplicarla a los contratos sin interés, generaría ${fmtQM(fin.intereses_no_cobrados)} adicionales — la captura actual es del ${fmtPct(fin.ratio_cobrado_vs_oportunidad)}.`);

    setHTML('finBreakdown', `
      <tr><td class="bold">Contado</td><td class="right">${fmtNum(fin.lotes_contado)}</td><td class="right">${fmtQ(fin.capital_contado)}</td><td class="right">—</td><td class="right">—</td></tr>
      <tr><td class="bold">Crédito sin interés</td><td class="right">${fmtNum(fin.lotes_sin_int)}</td><td class="right">${fmtQ(fin.capital_sin_int)}</td><td class="right" style="color:var(--muted)">${fmtQ(fin.intereses_sin_int_pagados)}</td><td class="right">${Number(fin.plazo_prom_sin_int||0).toFixed(0)} m</td></tr>
      <tr><td class="bold">Crédito con interés</td><td class="right">${fmtNum(fin.lotes_con_int)}</td><td class="right">${fmtQ(fin.capital_con_int)}</td><td class="right" style="color:var(--green);font-weight:700">${fmtQ(fin.intereses_cobrados)}</td><td class="right">${Number(fin.plazo_prom_con_int||0).toFixed(0)} m</td></tr>`);
  }

  if (v.vend && v.vend.length) {
    const top = v.vend.slice(0,10);
    setHTML('vendedoresTbody', top.map((vd,i)=>`
      <tr>
        <td class="bold">${i+1}</td>
        <td class="bold">${vd.vendedor}</td>
        <td><span class="pill ${vd.equipo==='CONSERSA'?'blue':vd.equipo==='RV4'?'amber':''}">${vd.equipo}</span></td>
        <td>${vd.proyecto}</td>
        <td class="right">${fmtNum(vd.ventas_brutas)}</td>
        <td class="right" style="color:var(--red)">${fmtNum(vd.desistimientos||0)}</td>
        <td class="right bold" style="color:var(--green)">${fmtNum(vd.ventas_netas)}</td>
        <td class="right">${fmtQ(vd.ticket_promedio)}</td>
      </tr>`).join(''));
  }

  if (v.metas && v.metas.length) {
    setHTML('metasRows', v.metas.map(m=>{
      const pct = Number(m.cumplimiento_pct||0);
      const color = pct>=80?'var(--green)':pct>=50?'var(--dorado)':'var(--red)';
      return `<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:18px 24px;margin-bottom:12px;display:grid;grid-template-columns:280px 200px 1fr 120px;gap:18px;align-items:center">
        <div><div style="font-size:15px;font-weight:700">${m.responsable}</div><div style="font-size:12px;color:var(--muted);font-weight:500">${m.proyecto}</div></div>
        <div style="font-size:13px"><div><strong>${fmtNum(m.ventas_total)}</strong> / ${fmtNum(m.meta_total)}</div><div style="font-size:11px;color:var(--muted);margin-top:2px">CONS: ${fmtNum(m.ventas_consersa||0)} · RV4: ${fmtNum(m.ventas_rv4||0)}</div></div>
        <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:${color};width:${Math.min(pct,100)}%"></div></div>
        <div style="text-align:right;font-size:18px;font-weight:700;color:${color}">${fmtPct(pct)}</div>
      </div>`;
    }).join(''));
  }

  if (v.tend && v.tend.length) drawTendencia(v.tend);
}

async function loadCartera() {
  const cqs = carteraPeriodParams();
  try {
    const [k, aging, proy, alertas, desist] = await Promise.all([
      apiFetch(`/api/cartera/kpis?${cqs}`),
      apiFetch(`/api/cartera/aging?${cqs}`),
      apiFetch(`/api/cartera/proyeccion-mensual?meses=12&${cqs}`),
      apiFetch('/api/cartera/alertas').catch(()=>null),
      apiFetch('/api/cartera/desistimientos?page_size=20').catch(()=>null),
    ]);
    state.data.cartera = { k, aging, proy, alertas, desist };
  } catch(e) {
    console.warn('loadCartera:', e);
    state.data.cartera = {};
  }
  await renderCarteraSlides();
}

async function renderCarteraSlides() {
  const c = state.data.cartera;
  if (!c) return;
  const k = c.k;
  if (k) {
    setText('carTotal', fmtQM(k.cartera_total));
    setText('carClientes', `${fmtNum(k.clientes_activos)} clientes activos`);
    setText('carMora', fmtQM(k.mora_total));
    setText('carMoraTasa', `Tasa ${fmtPct(k.tasa_mora)}`);
    setText('carCapital', fmtQM(k.capital_total));
    setText('carIntereses', fmtQM(k.intereses_total));
    setText('carCobro30', fmtQM(k.cobro_30d));
    setText('carCobro60', fmtQM(k.cobro_60d));
    setText('carCobro90', fmtQM(k.cobro_90d));
    setText('carVencidos', fmtNum(k.clientes_vencidos));
    const pctVenc = k.clientes_activos ? (k.clientes_vencidos/k.clientes_activos*100) : 0;
    setText('carPctVenc', `${pctVenc.toFixed(1)}% del total`);

    setText('desTotal', fmtNum(k.desistimientos_total));
    setText('desPagado', fmtQM(k.desistimientos_pagado));
    setText('desReint', fmtQM(k.desistimientos_reintegrado));
    const ret = k.desistimientos_pagado-k.desistimientos_reintegrado;
    setText('desRetencion', `Retenido por sociedad: ${fmtQM(ret)}`);
    setText('desLectura', `Históricamente, ${fmtNum(k.desistimientos_total)} desistimientos representaron ${fmtQM(k.desistimientos_pagado)} en pagos de clientes. Se reintegró ${fmtQM(k.desistimientos_reintegrado)} y la sociedad retuvo ${fmtQM(ret)} por concepto de penalizaciones contractuales.`);
  }
  if (c.aging) {
    setHTML('agingTbody', c.aging.map(a=>`<tr><td class="bold">${a.rango}</td><td class="right">${fmtNum(a.cuotas)}</td><td class="right">${fmtNum(a.clientes)}</td><td class="right bold" style="color:var(--red)">${fmtQ(a.monto)}</td></tr>`).join(''));
    drawAging(c.aging);
    const total = c.aging.reduce((s,a)=>s+Number(a.monto||0),0);
    const criticos = c.aging.filter(a=>a.rango.includes('+180')||a.rango.includes('91-180'));
    const monto = criticos.reduce((s,a)=>s+Number(a.monto||0),0);
    setText('agingLectura', `${fmtPct(total?monto/total*100:0)} del monto vencido (${fmtQM(monto)}) tiene más de 90 días — son los casos que requieren gestión inmediata o provisiones.`);
  }
  if (c.proy && c.proy.length) drawProyeccion(c.proy);
  if (c.alertas) {
    setText('alRoja', fmtNum(c.alertas.rojas));
    setText('alAmar', fmtNum(c.alertas.amarillas));
    setText('alTotal', fmtNum(c.alertas.total));
    const top = (c.alertas.alertas||[]).slice(0,8);
    setHTML('alertasTbody', top.map(a=>`<tr>
      <td><span class="pill ${a.nivel==='ROJO'?'red':'amber'}">${a.nivel}</span></td>
      <td class="bold">${humanType(a.tipo)}</td>
      <td>${a.mensaje}</td>
      <td style="font-size:13px;color:var(--text-soft)">${a.detalle}</td>
    </tr>`).join(''));
  }
}

async function renderPCVSlides() {
  const p = state.data.pcv;
  if (!p) return;
  const k = p.k;
  if (k) {
    setText('pcvTotal', fmtNum(k.total_ventas));
    setText('pcvCon', fmtNum(k.con_pcv));
    setText('pcvPct', `${fmtPct(k.pct_cumplimiento)} de cumplimiento`);
    setText('pcvSin', fmtNum(k.sin_pcv));
    setText('pcvSin2026', `${fmtNum(k.sin_pcv_2026)} en ${state.anio} · ${fmtPct(k.pct_sin_pcv_2026)}`);
    setText('pcvDias', `${Number(k.dias_prom_gestion||0).toFixed(0)}`);
    setText('pcv0', fmtNum(k.sin_pcv_0_15));
    setText('pcv15', fmtNum(k.sin_pcv_16_30));
    setText('pcv30', fmtNum(k.sin_pcv_31_90));
    setText('pcv90', fmtNum(k.sin_pcv_mas90));
  }
  if (p.reg) {
    setText('regRojas', fmtNum(p.reg.rojas));
    setText('regAmar', fmtNum(p.reg.amarillas));
    setText('regGris', fmtNum(p.reg.grises||0));
    setText('regTotal', fmtNum(p.reg.total));
    const top = (p.reg.issues||[]).slice(0,8);
    setHTML('regTbody', top.map(i=>`<tr>
      <td><span class="pill ${i.nivel==='ROJO'?'red':i.nivel==='AMARILLO'?'amber':''}">${i.nivel}</span></td>
      <td class="bold">${humanType(i.tipo)}</td>
      <td>${i.mensaje}<div style="font-size:12px;color:var(--muted);margin-top:4px">${i.detalle||''}</div></td>
      <td style="font-size:13px;color:var(--text-soft)">${i.accion||'—'}</td>
    </tr>`).join(''));
  }
}

/* ── Init ───────────────────────────────────────── */
function init() {
  // Theme
  applyTheme(localStorage.getItem('rv4_pres_theme') || 'light');
  document.getElementById('themeBtn').addEventListener('click', toggleTheme);

  // Period selectors
  document.getElementById('periodMes').value = state.mes;
  document.getElementById('periodAnio').value = state.anio;
  document.getElementById('periodMes').addEventListener('change', e => { state.mes  = Number(e.target.value); loadAll(); });
  document.getElementById('periodAnio').addEventListener('change', e => { state.anio = Number(e.target.value); loadAll(); });
  // Keepalive: ping every 20s to prevent backend session timeout
  setInterval(async () => {
    try {
      const token = getToken();
      if (token) await fetch('/api/auth/me', {
        headers: { 'Authorization': `Bearer ${token}` },
        signal: AbortSignal.timeout(5000)
      });
    } catch(e) { /* ignore keepalive errors */ }
  }, 20000);

  document.getElementById('flujoSociedad').addEventListener('change', e => { state.sociedad = e.target.value; if (getToken()) loadFlujos(); });
  document.getElementById('detalleFlujoSociedad')?.addEventListener('change', () => { if (getToken()) loadDetalleFlujos(); });

  // SSO from URL (?token=xxx&usuario=base64)
  const params = new URLSearchParams(window.location.search);
  if (params.get('token')) {
    localStorage.setItem('token', params.get('token'));
    if (params.get('usuario')) {
      try { localStorage.setItem('usuario', atob(params.get('usuario'))); } catch(e) {}
    }
    history.replaceState({}, '', window.location.pathname);
  }

  // Init defaults: current month/year if available
  const now = new Date();
  if (!params.get('mes') && !params.get('anio')) {
    state.anio = now.getFullYear();
    state.mes = 0;  // empieza con "todo el año" (Junta = balance anual)
    document.getElementById('periodAnio').value = state.anio;
    document.getElementById('periodMes').value  = state.mes;
  }

  fitStage();
  setTimeout(fitStage, 100);
  showSlide(0);
  loadAll();
}

document.addEventListener('DOMContentLoaded', init);
