/* ═══════════════════════════════════════════════════════════════
   RV4 — Presentación Junta Directiva Lotificadoras
   Conectada a la API del tablero (mismo token de localStorage)
   ═══════════════════════════════════════════════════════════════ */

const API = '';   // mismo origen que el tablero
const METAS_MENSUALES = {"Ottavia": 15, "Tezzoli": 8, "Eficiencia Urbana": 15, "Servicios Generales": 7, "Capipos": 5, "Urbiva": 5, "Corcolle": 10, "Frugalex": 8, "Ovest": 8, "Vilet": 0, "Rossio": 5, "Utilica": 8, "Garbatella": 7};
const META_TOTAL_MENSUAL = 101;
const METAS_POR_PROYECTO = {"Ottavia \u2014 Ca\u00f1adas de Jalapa": 15, "Tezzoli \u2014 Club Campestre Jumay": 8, "Eficiencia Urbana \u2014 Hacienda Jumay": 15, "Servicios Generales \u2014 La Ceiba": 7, "Capipos \u2014 Arboleda Santa Elena": 5, "Urbiva \u2014 Club del Bosque": 5, "Corcolle \u2014 Hacienda El Cafetal Fase I": 10, "Frugalex \u2014 Oasis Zacapa": 8, "Ovest \u2014 Hacienda Santa Lucia": 8, "Rossio \u2014 Hacienda el Sol": 5, "Utilica \u2014 Condado Jutiapa": 8, "Garbatella \u2014 Club Residencial El Progreso": 7};
const PROYECTO_DISPLAY_MAP = {"Ottavia": "Ottavia \u2014 Ca\u00f1adas de Jalapa", "Tezzoli": "Tezzoli \u2014 Club Campestre Jumay", "Eficiencia Urbana": "Eficiencia Urbana \u2014 Hacienda Jumay", "Hacienda Jumay": "Eficiencia Urbana \u2014 Hacienda Jumay", "Servicios Generales": "Servicios Generales \u2014 La Ceiba", "La Ceiba": "Servicios Generales \u2014 La Ceiba", "Capipos": "Capipos \u2014 Arboleda Santa Elena", "Arboleda Santa Elena": "Capipos \u2014 Arboleda Santa Elena", "Urbiva": "Urbiva \u2014 Club del Bosque", "Club del Bosque": "Urbiva \u2014 Club del Bosque", "Corcolle": "Corcolle \u2014 Hacienda El Cafetal Fase I", "Frugalex": "Frugalex \u2014 Oasis Zacapa", "Oasis Zacapa": "Frugalex \u2014 Oasis Zacapa", "Ovest": "Ovest \u2014 Hacienda Santa Lucia", "Hacienda Santa Lucia": "Ovest \u2014 Hacienda Santa Lucia", "Rossio": "Rossio \u2014 Hacienda el Sol", "Hacienda el Sol": "Rossio \u2014 Hacienda el Sol", "Utilica": "Utilica \u2014 Condado Jutiapa", "Condado Jutiapa": "Utilica \u2014 Condado Jutiapa", "Garbatella": "Garbatella \u2014 Club Residencial El Progreso", "Club Residencial Progreso": "Garbatella \u2014 Club Residencial El Progreso"}; // suma de metas por proyecto = 101
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
// Exponer state para módulo de Anexos (las funciones ya son globales por ser function declarations)
window.state = state;

/* ── Auth: usa el token del tablero ─────────────── */
function getToken() {
  return localStorage.getItem('token');
}

async function apiFetch(path, _retry = 0) {
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
    // Retry up to 2 times on network errors (ERR_NETWORK_CHANGED, connection drops)
    if (_retry < 2) {
      await new Promise(res => setTimeout(res, 500 * (_retry + 1)));
      return apiFetch(path, _retry + 1);
    }
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

/* ── Snapshot helpers ────────────────────────────── */
async function imgToBase64(img) {
  return new Promise(resolve => {
    if (!img.src || img.src.startsWith('data:')) { resolve(null); return; }
    const canvas = document.createElement('canvas');
    const doConvert = () => {
      try {
        canvas.width  = img.naturalWidth  || 64;
        canvas.height = img.naturalHeight || 64;
        canvas.getContext('2d').drawImage(img, 0, 0);
        resolve(canvas.toDataURL());
      } catch(e) { resolve(null); }
    };
    if (img.complete && img.naturalWidth > 0) doConvert();
    else { img.onload = doConvert; img.onerror = () => resolve(null); }
  });
}

/* ── Snapshot helpers ─────────────────────── */
async function embedAllImages(container) {
  const imgs = [...container.querySelectorAll('img[src]')];
  for (const img of imgs) {
    const src = img.getAttribute('src');
    if (!src || src.startsWith('data:')) continue;
    try {
      const res = await fetch(src);
      if (!res.ok) continue;
      const blob = await res.blob();
      const b64 = await new Promise(r => {
        const fr = new FileReader();
        fr.onload = () => r(fr.result);
        fr.readAsDataURL(blob);
      });
      img.src = b64;
    } catch(e) {}
  }
}

async function descargarPresentacion(formato) {
  document.getElementById('downloadMenu')?.classList.remove('open');
  const isPDF = (formato === 'pdf');
  showDlToast(isPDF ? 'Preparando PDF…' : 'Capturando todos los slides…');
  await new Promise(r => setTimeout(r, 150));

  try {
    // 1) LEER TEMA ACTIVO (claro/oscuro) — se replica en la descarga
    const theme = document.documentElement.getAttribute('data-theme') || 'light';

    // 2) Capturar TODO el CSS actual de la presentación
    const allCSS = [...document.querySelectorAll('style')].map(s => s.textContent).join('\n');

    // 3) Clonar todos los slides (visibles + ocultos los excluimos)
    const livSlides = [...document.querySelectorAll('.slide')].filter(s => !s.dataset.hidden);
    const snapParts = [];
    for (let i = 0; i < livSlides.length; i++) {
      const cl = livSlides[i].cloneNode(true);
      cl.removeAttribute('style');
      cl.className = 'snap-slide';
      // Quitar controles interactivos que no aplican en snapshot
      cl.querySelectorAll('select,.download-wrap,.tb-pill,.nav-arrow').forEach(e => e.remove());
      await embedAllImages(cl);
      snapParts.push(cl.outerHTML);
    }

    const periodo = periodoFormal();
    const N = snapParts.length;
    const si = Math.min(state.slide || 0, N - 1);

    // 4) Barra de navegación + JS — solo en modo HTML
    const navBar = isPDF ? '' : (
      '<div class="sbar">' +
      '<span class="stitle">JUNTA DIRECTIVA \u00b7 RV4</span>' +
      '<div style="display:flex;align-items:center;gap:10px">' +
      '<button class="sbtn" onclick="sP()">&#9664;</button>' +
      '<span class="scnt" id="sc">' + (si+1) + ' / ' + N + '</span>' +
      '<button class="sbtn" onclick="sN()">&#9654;</button>' +
      '</div>' +
      '<span class="speriod">\uD83D\uDCCE ' + periodo + ' \u00b7 ' + N + ' slides</span>' +
      '</div>' +
      '<div class="sarr sarr-l" onclick="sP()">\u2039</div>' +
      '<div class="sarr sarr-r" onclick="sN()">\u203a</div>'
    );

    const navJS = isPDF ? '' : (
      '<' + 'script>' +
      'var _i=' + si + ',_ss=document.querySelectorAll(".snap-slide");' +
      'function sS(i){_ss.forEach(function(x){x.style.display="none";});' +
      '_i=Math.max(0,Math.min(i,_ss.length-1));' +
      '_ss[_i].style.display="flex";' +
      'document.getElementById("sc").textContent=(_i+1)+" / "+_ss.length;}' +
      'function sN(){sS(_i+1);}function sP(){sS(_i-1);}' +
      'document.addEventListener("keydown",function(e){if(e.key==="ArrowRight"||e.key===" ")sN();else if(e.key==="ArrowLeft")sP();});' +
      'var tx=0;' +
      'document.addEventListener("touchstart",function(e){tx=e.touches[0].clientX;},{passive:true});' +
      'document.addEventListener("touchend",function(e){var d=tx-e.changedTouches[0].clientX;if(Math.abs(d)>40){d>0?sN():sP();}});' +
      'sS(' + si + ');' +
      '</' + 'script>'
    );

    // 5) CSS específico del modo
    //    - HTML: slide ocupa pantalla completa con scale dinámico
    //    - PDF:  slide FIJO 1920×1080 (16:9 landscape) para impresión a PDF
    const slideCSS = isPDF
      ? [
          // Reglas @page para PDF 16:9 (1920x1080 = 20 x 11.25 inch @ 96 dpi)
          '@page { size: 1920px 1080px landscape; margin: 0; }',
          '@media print { html, body { width: 1920px; height: auto; overflow: visible !important; background: var(--bg) !important; } }',
          '.snap-slide{display:flex;flex-direction:column;width:1920px;height:1080px;overflow:hidden;background:var(--bg-section);page-break-after:always;page-break-inside:avoid;position:relative;margin:0}',
          '.snap-slide:last-child{page-break-after:auto}'
        ].join('\n')
      : [
          // En HTML: slide visible es un viewport-fit; rest oculto
          '.snap-slide{display:none;flex-direction:column;width:100vw;height:100vh;overflow:hidden;background:var(--bg-section);position:fixed;inset:0;padding-top:42px}'
        ].join('\n');

    // 6) Estilos de barra (tema-aware via variables)
    const barCSS = isPDF ? '' : [
      '.sbar{position:fixed;top:0;left:0;right:0;z-index:9999;height:42px;background:var(--bg-card);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 16px}',
      '.stitle{font-size:11px;color:var(--dorado);font-weight:700;letter-spacing:.06em}',
      '.speriod{font-size:11px;color:var(--text-soft)}',
      '.sbtn{background:var(--bg-section);border:1px solid var(--border);color:var(--text);padding:4px 14px;border-radius:5px;cursor:pointer;font-size:13px;font-family:inherit}',
      '.sbtn:hover{background:var(--dorado);color:var(--negro)}',
      '.scnt{font-size:13px;font-weight:700;color:var(--text);min-width:56px;text-align:center}',
      '.sarr{position:fixed;top:50%;transform:translateY(-50%);z-index:9998;background:var(--bg-card);border:1px solid var(--border);color:var(--text-soft);width:32px;height:64px;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:20px}',
      '.sarr:hover{background:var(--dorado);color:var(--negro)}',
      '.sarr-l{left:3px;border-radius:0 8px 8px 0}.sarr-r{right:3px;border-radius:8px 0 0 8px}',
      '@media print{.sbar,.sarr{display:none!important}html,body{overflow:auto;height:auto}.snap-slide{display:flex!important;position:relative;page-break-after:always}.snap-slide:last-child{page-break-after:auto}}'
    ].join('\n');

    // 7) Construir el HTML final — data-theme replica el tema activo
    const html = [
      '<!DOCTYPE html>',
      '<html lang="es" data-theme="' + theme + '">',
      '<head>',
      '<meta charset="UTF-8">',
      '<title>Presentacion JD RV4 -- ' + periodo + '</title>',
      '<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">',
      '<style>',
      // Reset mínimo — NO override de variables, las heredan del allCSS original
      'html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font-family:Montserrat,sans-serif;' + (isPDF ? '' : 'overflow:hidden;') + 'height:' + (isPDF ? 'auto' : '100%') + '}',
      '*,*::before,*::after{box-sizing:border-box}',
      // Todas las variables (claro y oscuro) y estilos originales
      allCSS,
      slideCSS,
      barCSS,
      '</style>',
      '</head>',
      '<body>',
      navBar,
      snapParts.join('\n'),
      navJS,
      '</body>',
      '</html>'
    ].join('\n');

    const blob = new Blob([html], {type:'text/html;charset=utf-8'});
    const url = URL.createObjectURL(blob);

    if (isPDF) {
      // 8) PDF: abrir en pestaña nueva → window.print() automático
      //    El navegador respetará @page size 1920x1080 landscape
      const win = window.open(url, '_blank');
      if (win) {
        win.addEventListener('load', function() {
          setTimeout(function() {
            try { win.print(); } catch(e) { console.warn('print failed', e); }
            setTimeout(function() { URL.revokeObjectURL(url); }, 2000);
          }, 1200);
        });
        hideDlToast();
      } else {
        // Si el popup fue bloqueado, ofrecer descarga manual
        const a = document.createElement('a');
        a.href = url;
        a.download = 'PresentacionJD_PDF_' + state.anio + '_' + String(state.mes||0).padStart(2,'0') + '.html';
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
        showDlToast('Popup bloqueado. Abrí el HTML y presioná Ctrl+P', false);
      }
    } else {
      // 9) HTML: descarga directa
      const a = document.createElement('a');
      a.href = url;
      a.download = 'PresentacionJD_RV4_' + state.anio + '_' + String(state.mes||0).padStart(2,'0') + '_' + periodo.replace(/\s+/g,'_') + '.html';
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(url);
      hideDlToast();
    }

  } catch(e) {
    showDlToast('Error: ' + e.message, true);
    console.error('Snapshot error:', e);
  }
}


/* ── Navegación ─────────────────────────────────── */
/* ── Flujos: split Intercompany from Financiamiento + sort ── */
function procesarFilasFlujos(filas, secciones, icOverride) {
  const out = [];
  for (const f of filas) {
    if (f.seccion === 'FINANCIAMIENTO') {
      let icIng = 0, icEgr = 0;
      let canSplit = false;

      if (icOverride) {
        // CONSOLIDADO: only split if IC values are within FINANCIAMIENTO bounds
        // to avoid negative egreso rows that break totals
        icIng = icOverride.ingreso || 0;
        icEgr = icOverride.egreso  || 0;
        canSplit = (icIng <= f.ingreso + 1) && (icEgr <= f.egreso + 1);
      } else {
        // Single society: find Intercompany in secciones
        const secData = (secciones || []).find(s => s.seccion === 'FINANCIAMIENTO');
        const cats = secData?.categorias || [];
        const icCat = cats.find(c => c.categoria?.toLowerCase().includes('intercompany'));
        if (icCat) {
          Object.values(icCat.montos || {}).forEach(m => {
            icIng += m.ingreso || 0;
            icEgr += m.egreso  || 0;
          });
          canSplit = (icIng <= f.ingreso + 1) && (icEgr <= f.egreso + 1);
        }
      }

      if (canSplit && (icIng > 0 || icEgr > 0)) {
        const icNeto = icIng - icEgr;
        const finRow = { seccion:'FINANCIAMIENTO', ingreso:f.ingreso-icIng, egreso:f.egreso-icEgr, neto:f.neto-icNeto };
        const icRow  = { seccion:'INTERCOMPANY',   ingreso:icIng,           egreso:icEgr,          neto:icNeto };
        if (Math.abs(finRow.ingreso)+Math.abs(finRow.egreso) > 0) out.push(finRow);
        if (Math.abs(icRow.ingreso) +Math.abs(icRow.egreso)  > 0) out.push(icRow);
      } else {
        // Can't split cleanly — show as single FINANCIAMIENTO row
        // but rename to show IC is included
        out.push({ ...f, seccion: 'FINANCIAMIENTO (inc. Intercompany)' });
      }
    } else {
      out.push(f);
    }
  }
  // Sort: INGRESOS always first, then positives desc, then negatives desc by |neto|
  const sectionRank = sec => {
    if (sec === 'INGRESOS') return 0;
    return 1;
  };
  out.sort((a, b) => {
    const rankDiff = sectionRank(a.seccion) - sectionRank(b.seccion);
    if (rankDiff !== 0) return rankDiff;
    const aPos = a.neto >= 0, bPos = b.neto >= 0;
    if (aPos && !bPos) return -1;
    if (!aPos && bPos) return 1;
    return Math.abs(b.neto) - Math.abs(a.neto);
  });
  return out;
}

function showSlide(idx) {
  const slides = [...document.querySelectorAll('.slide')].filter(s => !s.dataset.hidden);
  state.total = slides.length;
  if (idx < 0) idx = 0;
  if (idx >= slides.length) idx = slides.length - 1;
  state.slide = idx;
  // Hide all slides, show active
  document.querySelectorAll('.slide').forEach(s => s.classList.remove('active'));
  slides[idx]?.classList.add('active');
  // Update counter
  document.getElementById('tbCount').textContent = `${idx + 1} / ${slides.length}`;
  // Update all slide-num elements dynamically
  slides.forEach((s, i) => {
    const numEl = s.querySelector('.slide-num');
    if (numEl) {
      // Skip portada (index 0) and dividers
      const label = s.dataset.screenLabel || '';
      const isDivider = label.toLowerCase().includes('divider') || label.toLowerCase().includes('portada');
      numEl.textContent = isDivider ? '' : `${String(i+1).padStart(2,'0')} / ${String(slides.length).padStart(2,'0')}`;
    }
  });
  // Update agenda numbers based on visible sections
  updateAgendaNumbers();
  try { window.parent.postMessage({ slideIndexChanged: idx }, '*'); } catch(e) {}
}

function updateAgendaNumbers() {
  // Renumber agenda items based on which sections have visible slides
  const seccionesVisibles = new Set();
  document.querySelectorAll('.slide:not([data-hidden])').forEach(s => {
    const sec = s.dataset.seccion;
    if (sec) seccionesVisibles.add(sec);
  });
  let num = 1;
  document.querySelectorAll('.agenda-item[data-agenda-seccion]').forEach(item => {
    const sec = item.dataset.agendaSeccion;
    if (seccionesVisibles.has(sec)) {
      item.style.display = '';
      const numEl = item.querySelector('.agenda-n');
      if (numEl) numEl.textContent = String(num).padStart(2,'0');
      num++;
    } else {
      item.style.display = 'none';
    }
  });
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
  // "Valor venta al contado" subtitle below disponibles value
  const invDispValEl = document.getElementById('invDispVal');
  if (invDispValEl) {
    const existing = document.getElementById('invDispSubLabel');
    if (!existing) {
      const sub = document.createElement('div');
      sub.id = 'invDispSubLabel';
      sub.style.cssText = 'font-size:10px;color:var(--muted);font-weight:600;letter-spacing:.05em;margin-top:2px';
      sub.textContent = 'Valor venta al contado';
      invDispValEl.parentNode.insertBefore(sub, invDispValEl.nextSibling);
    }
  }
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
    return `<div class="bar-row" style="padding:${sorted.length > 10 ? '8px 0' : '13px 0'}">
      <div class="bar-name" style="font-size:${sorted.length > 10 ? '12px' : '14px'}">${p.nombre_proyecto}<div style="font-size:11px;color:var(--muted);font-weight:500;margin-top:1px">${p.nombre_sociedad}</div></div>
      <div class="bar-track" style="height:${sorted.length > 10 ? '10px' : '14px'}"><div class="bar-fill" style="width:${Math.min(pct,100)}%"></div></div>
      <div class="bar-pct" style="font-size:${sorted.length > 10 ? '14px' : '16px'};min-width:100px">${fmtPct(pct)}<div style="font-size:11px;color:var(--muted);font-weight:500;margin-top:1px;text-align:right">${vendidos}/${total}</div></div>
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
    return `<tr style="line-height:1.2">
      <td class="bold" style="padding:8px 12px">${i+1}</td>
      <td class="bold" style="padding:8px 12px">${p.nombre_proyecto}</td>
      <td style="padding:8px 12px">${p.nombre_sociedad}</td>
      <td class="right" style="padding:8px 12px">${fmtNum(disp)}</td>
      <td class="right bold" style="padding:8px 12px">${fmtQ(valDisp)}</td>
      <td class="right" style="padding:8px 12px;color:var(--dorado);font-weight:700">${fmtQ(ticket)}</td>
      <td class="right" style="padding:8px 12px"><span class="pill ${cls}">${fmtPct(pct)}</span></td>
    </tr>`;
  }).join(''));
  // Populate tendencia project filter — format: "Sociedad — Proyecto" using known mapping
  const tendSelect = document.getElementById('tendenciaProyecto');
  if (tendSelect && proyectos.length) {
    // Mapping of known proyecto names to display labels
    const PROYECTO_LABEL_MAP = {
      'Hacienda Jumay':              'Eficiencia Urbana — Hacienda Jumay',
      'La Ceiba':                    'Servicios Generales — La Ceiba',
      'Hacienda el Sol':             'Rossio — Hacienda el Sol',
      'Oasis Zacapa':                'Frugalex — Oasis Zacapa',
      'Cañadas de Jalapa':           'Ottavia — Cañadas de Jalapa',
      'Condado Jutiapa':             'Utilica — Condado Jutiapa',
      'Club Campestre Jumay':        'Tezzoli — Club Campestre Jumay',
      'Club del Bosque':             'Urbiva — Club del Bosque',
      'Club Residencial Progreso':   'Garbatella — Club Residencial El Progreso',
      'Arboleda Santa Elena':        'Capipos — Arboleda Santa Elena',
      'Hacienda Santa Lucia':        'Ovest — Hacienda Santa Lucia',
      'Hacienda El Cafetal Fase I':  'Corcolle — Hacienda El Cafetal Fase I',
      'Hacienda El Cafetal Fase III':'Gibraleón — Hacienda El Cafetal Fase III',
    };
    const opts = '<option value="">Consolidado (Todos)</option>' +
      [...proyectos].sort((a,b) => a.nombre_proyecto.localeCompare(b.nombre_proyecto))
        .map(p => {
          const nombre = p.nombre_proyecto || '';
          // Try exact match, then partial, then fallback to sociedad field
          let label = PROYECTO_LABEL_MAP[nombre];
          if (!label && p.sociedad) label = `${p.sociedad} — ${nombre}`;
          if (!label) label = nombre;
          return `<option value="${nombre}">${label}</option>`;
        }).join('');
    tendSelect.innerHTML = opts;
  }
}

async function reloadTendencia() {
  const proy = document.getElementById('tendenciaProyecto')?.value || '';
  const proyParam = proy ? `&proyecto=${encodeURIComponent(proy)}` : '';
  const tend = await apiFetch(`/api/ventas/tendencia-mensual?meses_atras=12&año=${state.anio}${state.mes > 0 ? '&mes='+state.mes : ''}${proyParam}`);
  if (tend && tend.length) {
    const proy2 = document.getElementById('tendenciaProyecto')?.value || '';
    let metaVal = 0;
    if (!proy2) {
      metaVal = META_TOTAL_MENSUAL; // consolidated = 101
    } else {
      // proy2 = nombre BD (e.g. "Hacienda Jumay"). METAS_MENSUALES usa sociedad short (e.g. "Eficiencia Urbana").
      // First try direct key match (sociedad short names like "Ottavia"), then partial
      const NOMBRE_BD_A_META_KEY = {
        'Hacienda Jumay':              'Eficiencia Urbana',
        'La Ceiba':                    'Servicios Generales',
        'Hacienda el Sol':             'Rossio',
        'Oasis Zacapa':                'Frugalex',
        'Cañadas de Jalapa':           'Ottavia',
        'Condado Jutiapa':             'Utilica',
        'Club Campestre Jumay':        'Tezzoli',
        'Club del Bosque':             'Urbiva',
        'Club Residencial Progreso':   'Garbatella',
        'Club Residencial El Progreso':'Garbatella',
        'Arboleda Santa Elena':        'Capipos',
        'Hacienda Santa Lucia':        'Ovest',
        'Hacienda El Cafetal Fase I':  'Corcolle',
        'Celajes De Tecpan':           'Vilet',
      };
      // Try direct BD name lookup first
      const metaKey = NOMBRE_BD_A_META_KEY[proy2];
      if (metaKey) {
        metaVal = METAS_MENSUALES[metaKey] || 0;
      } else {
        // Fallback: partial match against METAS_MENSUALES keys
        const match = Object.entries(METAS_MENSUALES).find(([k]) =>
          proy2.toLowerCase().includes(k.toLowerCase()) || k.toLowerCase().includes(proy2.toLowerCase())
        );
        metaVal = match ? match[1] : 0;
      }
    }
    drawTendencia(tend, metaVal);
  } else {
    const el = document.getElementById('tendenciaChart');
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:300px;color:var(--muted);font-size:14px">Sin datos de tendencia para este proyecto</div>';
  }
}

/* ── Ventas ─────────────────────────────────────── */
async function loadVentas() {
  const qs = periodParams();
  const [k, mezcla, fin, vend, metas, tend] = await Promise.all([
    apiFetch(`/api/ventas/kpis?${qs}`),
    apiFetch(`/api/ventas/mezcla-financiera?${state.mes ? '' : 'todo_el_tiempo=false&'}año=${state.anio}&meses_atras=12`),
    apiFetch(`/api/ventas/analisis-financiero?${qs}`),
    apiFetch(`/api/ventas/por-vendedor?${qs}`),
    apiFetch(`/api/ventas/metas?año=${state.anio}${state.mes > 0 ? '&mes='+state.mes : ''}`),
    apiFetch(`/api/ventas/tendencia-mensual?meses_atras=12&año=${state.anio}${state.mes > 0 ? '&mes='+state.mes : ''}`)
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
    // Compute intereses sin cobrar (oportunidad) using new formula:
    // (contado + sin_interés) × ticket_promedio × 10% anual × (plazo_promedio / 12)
    const udsNoInt    = (k.contado||0) + (k.sin_interes||0);
    const montoNoInt  = udsNoInt * (k.ticket_promedio||0);
    const plazoAnios  = (k.plazo_promedio||0) / 12;
    window._oportunidadCalculada = Math.round(montoNoInt * 0.10 * plazoAnios);
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
        <div><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;color:var(--blue);margin-bottom:4px"><span>Capital contado</span><span>${fmtQ(fin.capital_contado||0)}</span></div>
          <div style="height:10px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--blue);width:${pctOf(fin.capital_contado||0, cap_total)}%"></div></div>
        </div>
        <div><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;color:var(--green);margin-bottom:4px"><span>Intereses x cobrar</span><span>${fmtQ(fin.intereses_cobrados)}</span></div>
          <div style="height:10px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--green);width:${pctOf(fin.intereses_cobrados, fin.intereses_cobrados+(window._oportunidadCalculada||fin.intereses_no_cobrados||0))}%"></div></div>
        </div>
        <div><div style="display:flex;justify-content:space-between;font-size:12px;font-weight:600;color:var(--red);margin-bottom:4px"><span>Intereses sin cobrar (oportunidad)</span><span>${fmtQ((window._oportunidadCalculada||fin.intereses_no_cobrados||0))}</span></div>
          <div style="height:10px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--red);width:${pctOf((window._oportunidadCalculada||fin.intereses_no_cobrados||0), fin.intereses_cobrados+(window._oportunidadCalculada||fin.intereses_no_cobrados||0))}%"></div></div>
        </div>
        <div style="padding-top:8px;border-top:1px solid var(--border);font-size:12px;color:var(--muted);font-weight:500">Tasa anual implícita: <strong style="color:var(--dorado)">${fmtPct(fin.tasa_anual_implicita)}</strong> · Captura: <strong>${fmtPct(fin.ratio_cobrado_vs_oportunidad)}</strong></div>
      </div>`;
    setHTML('vtFinanciero', html);

    // Slide 11
    setText('finTasa', fmtPct(fin.tasa_anual_implicita));
    setText('finCobrados', fmtQ(fin.intereses_cobrados));  // label updated in HTML to 'Intereses x Cobrar'
    setText('finCobLotes', `${fmtNum(fin.lotes_con_int)} contratos con interés`);
    setText('finNoCobrados', fmtQ((window._oportunidadCalculada||fin.intereses_no_cobrados||0)));
    setText('finNoCobLotes', `${fmtNum(fin.lotes_sin_int)} contratos sin interés`);

    const lect = `La tasa implícita anual de los contratos con interés es ${fmtPct(fin.tasa_anual_implicita)}. De aplicarla a los contratos sin interés, generaría ${fmtQM((window._oportunidadCalculada||fin.intereses_no_cobrados||0))} adicionales — la captura actual es del ${fmtPct(fin.ratio_cobrado_vs_oportunidad)}.`;
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
    // Team totals footer — sumas del top 10 visible
    const equipos = {};
    top.forEach(v => {
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

  // Slide 13 — Metas vs Avance (tabla por proyecto × mes, sin CONSERSA)
  if (metas && metas.length) {
    const todoAnio = !state.mes || state.mes === 0;
    setText('metasSub', todoAnio
      ? `Cumplimiento de metas anuales · ${state.anio}`
      : `Cumplimiento de metas · ${MESES[state.mes]} ${state.anio}`);

    const hoy = new Date();
    const ultimoMes = (state.anio === hoy.getFullYear()) ? hoy.getMonth() + 1 : 12;
    const mesesVisibles = state.mes && state.mes > 0
      ? [state.mes]
      : Array.from({length: ultimoMes}, (_, i) => i + 1);
    const mesNombres = ['','Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];

    // MAPA OFICIAL de metas por mes (definido por el usuario, no del API que suma consersa+rv4)
    // Clave = proyecto_display que devuelve el API en campo "proyecto_display"
    const META_MES_OFICIAL = {
      'Ottavia — Cañadas de Jalapa':                15,
      'Tezzoli — Club Campestre Jumay':              8,
      'Eficiencia Urbana — Hacienda Jumay':          15,
      'Servicios Generales — La Ceiba':              7,
      'Capipos — Arboleda Santa Elena':              5,
      'Urbiva — Club del Bosque':                    5,
      'Corcolle — Hacienda El Cafetal Fase I':       10,
      'Frugalex — Oasis Zacapa':                     8,
      'Ovest — Hacienda Santa Lucia':                8,
      'Vilet — Celajes De Tecpan':                   0,
      'Rossio — Hacienda el Sol':                    5,
      'Utilica — Condado Jutiapa':                   8,
      'Garbatella — Club Residencial El Progreso':   7,
    };

    // Incluir proyectos con meta oficial O con ventas en el período
    const proyectosFiltrados = metas.filter(m => {
      const displayName = m.proyecto_display || m.proyecto || m.nombre_proyecto_bd || '';
      const metaOficial = META_MES_OFICIAL[displayName] ?? META_MES_OFICIAL[m.proyecto] ?? null;
      return metaOficial !== null || (m.ventas_total||0) > 0;
    });
    proyectosFiltrados.sort((a,b) => {
      const nameA = a.proyecto_display || a.proyecto || '';
      const nameB = b.proyecto_display || b.proyecto || '';
      const ma = META_MES_OFICIAL[nameA] ?? 0, mb = META_MES_OFICIAL[nameB] ?? 0;
      if (mb !== ma) return mb - ma;
      return (b.ventas_total||0) - (a.ventas_total||0);
    });

    const colsHeader = mesesVisibles.map(m =>
      `<th style="padding:6px 7px;text-align:center;font-size:11px;color:var(--muted);min-width:60px">${mesNombres[m]}</th>`
    ).join('');

    // Totales por mes (para el tfoot)
    const totMes = {};
    mesesVisibles.forEach(m => { totMes[m] = 0; });
    let totalMetaPeriodo = 0, totalRealPeriodo = 0;

    const rows = proyectosFiltrados.map(m => {
      const displayName = m.proyecto_display || m.proyecto || m.nombre_proyecto_bd || '—';
      // Usar meta oficial del mapa hardcodeado (no del API)
      const metaMensual = META_MES_OFICIAL[displayName] ?? META_MES_OFICIAL[m.proyecto] ?? 0;
      const porMes = m.por_mes || {};
      const mesCells = mesesVisibles.map(mes => {
        const real = Number(porMes[mes] || 0);
        totMes[mes] = (totMes[mes]||0) + real;
        const meta = metaMensual;
        const cumple = meta > 0 ? real >= meta : real > 0;
        const realColor = meta === 0 ? 'var(--blue)' : (cumple ? 'var(--green)' : 'var(--red)');
        return `<td style="padding:5px 7px;text-align:center">
          <div style="color:var(--muted);font-size:10px;line-height:1">M ${meta}</div>
          <div style="color:${realColor};font-weight:700;font-size:13px;line-height:1.2">${real}</div>
        </td>`;
      }).join('');
      const metaTotalProy = metaMensual * mesesVisibles.length;
      const realTotalProy = mesesVisibles.reduce((s,mes) => s + Number(porMes[mes]||0), 0);
      totalMetaPeriodo += metaTotalProy;
      totalRealPeriodo += realTotalProy;
      const pct = metaTotalProy > 0 ? Math.round(realTotalProy / metaTotalProy * 100) : 0;
      const barColor = pct >= 80 ? 'var(--green)' : pct >= 50 ? 'var(--dorado)' : 'var(--red)';
      return `<tr style="border-bottom:1px solid var(--border)">
        <td style="padding:7px 12px;font-size:12px;font-weight:600;white-space:nowrap;min-width:260px">${displayName}</td>
        ${mesCells}
        <td style="padding:7px 9px;text-align:center;font-weight:700;font-size:13px;color:var(--dorado)">${metaTotalProy}</td>
        <td style="padding:7px 9px;text-align:center;font-weight:700;font-size:13px;color:var(--blue)">${realTotalProy}</td>
        <td style="padding:7px 9px;min-width:120px">
          <div style="height:8px;background:rgba(255,255,255,.08);border-radius:4px;overflow:hidden"><div style="height:100%;background:${barColor};width:${Math.min(pct,100)}%"></div></div>
          <div style="font-size:11px;font-weight:700;color:${barColor};text-align:right;margin-top:2px">${pct}%</div>
        </td>
      </tr>`;
    }).join('');

    const totMesCells = mesesVisibles.map(m =>
      `<td style="padding:6px 7px;text-align:center;font-weight:700;font-size:13px;color:var(--blue)">${totMes[m]||0}</td>`
    ).join('');
    const totalPct = totalMetaPeriodo > 0 ? Math.round(totalRealPeriodo/totalMetaPeriodo*100) : 0;
    const totColor = totalPct>=80?'var(--green)':totalPct>=50?'var(--dorado)':'var(--red)';

    setHTML('metasRows', `<table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead><tr style="background:var(--azul-marino,#0d2340)">
        <th style="padding:7px 12px;text-align:left;font-size:12px;color:#fff;white-space:nowrap">Sociedad / Proyecto</th>
        ${colsHeader}
        <th style="padding:7px 9px;text-align:center;font-size:11px;color:var(--dorado)">Meta</th>
        <th style="padding:7px 9px;text-align:center;font-size:11px;color:var(--blue)">Real</th>
        <th style="padding:7px 9px;text-align:center;font-size:11px;color:var(--muted)">Avance</th>
      </tr></thead>
      <tbody>${rows}</tbody>
      <tfoot><tr style="background:var(--bg-section);font-weight:700">
        <td style="padding:7px 12px;font-size:13px">TOTAL</td>
        ${totMesCells}
        <td style="padding:7px 9px;text-align:center;color:var(--dorado);font-size:13px">${totalMetaPeriodo}</td>
        <td style="padding:7px 9px;text-align:center;color:var(--blue);font-size:13px">${totalRealPeriodo}</td>
        <td style="padding:7px 9px"><div style="height:8px;background:rgba(255,255,255,.08);border-radius:4px;overflow:hidden"><div style="height:100%;background:${totColor};width:${Math.min(totalPct,100)}%"></div></div><div style="font-size:11px;font-weight:700;color:${totColor};text-align:right;margin-top:2px">${totalPct}%</div></td>
      </tr></tfoot>
    </table>`);
  } // end metas block

  // Slide 10 — Tendencia mensual (chart)
  if (tend && tend.length) {
    const proy2 = document.getElementById('tendenciaProyecto')?.value || '';
    let metaVal = 0;
    if (!proy2) {
      metaVal = META_TOTAL_MENSUAL; // consolidated = 101
    } else {
      const NOMBRE_BD_A_META_KEY = {
        'Hacienda Jumay':'Eficiencia Urbana','La Ceiba':'Servicios Generales',
        'Hacienda el Sol':'Rossio','Oasis Zacapa':'Frugalex',
        'Cañadas de Jalapa':'Ottavia','Condado Jutiapa':'Utilica',
        'Club Campestre Jumay':'Tezzoli','Club del Bosque':'Urbiva',
        'Club Residencial Progreso':'Garbatella','Club Residencial El Progreso':'Garbatella',
        'Arboleda Santa Elena':'Capipos','Hacienda Santa Lucia':'Ovest',
        'Hacienda El Cafetal Fase I':'Corcolle','Celajes De Tecpan':'Vilet',
      };
      const metaKey = NOMBRE_BD_A_META_KEY[proy2];
      if (metaKey) {
        metaVal = METAS_MENSUALES[metaKey] || 0;
      } else {
        const match = Object.entries(METAS_MENSUALES).find(([k]) =>
          proy2.toLowerCase().includes(k.toLowerCase()) || k.toLowerCase().includes(proy2.toLowerCase())
        );
        metaVal = match ? match[1] : 0;
      }
    }
    drawTendencia(tend, metaVal);
  } else {
    const el = document.getElementById('tendenciaChart');
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:300px;color:var(--muted);font-size:14px">Sin datos de tendencia para el período seleccionado</div>';
  }
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
      let tgtPeriodos;
      if (state.mes > 0) {
        const cand = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
        tgtPeriodos = r.periodos.includes(cand) ? [cand] : [r.periodos[r.periodos.length-1]];
      } else {
        tgtPeriodos = r.periodos.filter(p => p.startsWith(String(state.anio)));
        if (!tgtPeriodos.length) tgtPeriodos = [r.periodos[r.periodos.length-1]];
      }
      (r.secciones || []).forEach(sec => {
        (sec.categorias || []).forEach(cat => {
          for (const tgt of tgtPeriodos) {
            const cv = cat.montos?.[tgt];
            if (cv?.ingreso > 0) ingrMapC[`${sec.seccion}||${cat.categoria}`] = (ingrMapC[`${sec.seccion}||${cat.categoria}`]||0) + cv.ingreso;
            if (cv?.egreso  > 0) egrMapC[`${sec.seccion}||${cat.categoria}`]  = (egrMapC[`${sec.seccion}||${cat.categoria}`]||0)  + cv.egreso;
          }
        });
      });
    });
    renderDetalleFlujosTables(ingrMapC, egrMapC);
    return;
  }
  // Always use mes granularity to correctly accumulate year totals
  const r = await apiFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(soc)}&granularidad=mes`);
    if (!r || !r.periodos) return;
    let tgtPeriodos;
    if (state.mes > 0) {
      const cand = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
      tgtPeriodos = r.periodos.includes(cand) ? [cand] : [r.periodos[r.periodos.length-1]];
    } else {
      tgtPeriodos = r.periodos.filter(p => p.startsWith(String(state.anio)));
      if (!tgtPeriodos.length) tgtPeriodos = [r.periodos[r.periodos.length-1]];
    }
    const ingrMap = {}, egrMap = {};
    (r.secciones || []).forEach(sec => {
      (sec.categorias || []).forEach(cat => {
        for (const target of tgtPeriodos) {
          const cv = cat.montos?.[target];
          if (cv?.ingreso > 0) ingrMap[`${sec.seccion}||${cat.categoria}`] = (ingrMap[`${sec.seccion}||${cat.categoria}`] || 0) + (cv.ingreso || 0);
          if (cv?.egreso  > 0) egrMap[`${sec.seccion}||${cat.categoria}`]  = (egrMap[`${sec.seccion}||${cat.categoria}`]  || 0) + (cv.egreso  || 0);
        }
      });
    });
    renderDetalleFlujosTables(ingrMap, egrMap);
}

function renderDetalleFlujosTables(ingrMap, egrMap) {
  const topN = 8;
  // Collect all unique category keys
  const allKeys = new Set([...Object.keys(ingrMap), ...Object.keys(egrMap)]);
  const cleanIngr = [], cleanEgr = [];

  allKeys.forEach(key => {
    const [sec] = key.split('||');
    const ing = ingrMap[key] || 0;
    const egr = egrMap[key] || 0;
    const isFinanciamiento = sec.toUpperCase().includes('FINANCIAMIENTO');

    if (isFinanciamiento) {
      // FINANCIAMIENTO: keep both sides separate (intercompany, préstamos)
      if (ing > 0) cleanIngr.push([key, ing]);
      if (egr > 0) cleanEgr.push([key, egr]);
    } else if (ing > 0 && egr > 0) {
      // Category appears in both → show only the net where it belongs
      const neto = ing - egr;
      if (neto > 0) cleanIngr.push([key, neto]);
      else if (neto < 0) cleanEgr.push([key, Math.abs(neto)]);
      // neto === 0 → don't show (traslados entre cuentas, etc.)
    } else {
      if (ing > 0) cleanIngr.push([key, ing]);
      if (egr > 0) cleanEgr.push([key, egr]);
    }
  });

  // Sort by value desc and take top N
  const ingrRows = cleanIngr.sort((a,b) => b[1] - a[1]).slice(0, topN);
  const egrRows  = cleanEgr.sort((a,b) => b[1] - a[1]).slice(0, topN);

  const toRow = ([key, val]) => {
    const [sec, cat] = key.split('||');
    return `<tr><td style="font-size:11px;color:var(--muted)">${sec.replace('EGRESOS / ','').replace('INGRESOS','OPERACIÓN')}</td><td>${cat}</td><td class="right bold">${fmtQ(val)}</td></tr>`;
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
      let targetPeriodos;
      if (state.mes > 0) {
        const cand = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
        targetPeriodos = r.periodos.includes(cand) ? [cand] : [r.periodos[r.periodos.length-1]];
      } else {
        // All months of the selected year
        targetPeriodos = r.periodos.filter(p => p.startsWith(String(state.anio)));
        if (!targetPeriodos.length) targetPeriodos = [r.periodos[r.periodos.length-1]];
      }
      const firstT = targetPeriodos[0];
      const lastT  = targetPeriodos[targetPeriodos.length-1];
      totalIni += r.saldos_iniciales[firstT] || 0;
      totalFin += r.saldos_finales[lastT]    || 0;
      (r.secciones || []).forEach(sec => {
        for (const tp of targetPeriodos) {
          const t = sec.totales[tp];
          if (!t) continue;
          totalIng += t.ingreso || 0;
          totalEgr += t.egreso  || 0;
          if (!secMap[sec.seccion]) secMap[sec.seccion] = { ingreso:0, egreso:0 };
          secMap[sec.seccion].ingreso += t.ingreso || 0;
          secMap[sec.seccion].egreso  += t.egreso  || 0;
        }
        // Track Intercompany totals for split display
        if (sec.seccion === 'FINANCIAMIENTO') {
          (sec.categorias || []).filter(c => c.categoria?.toLowerCase().includes('intercompany')).forEach(cat => {
            for (const tp of targetPeriodos) {
              const m = cat.montos?.[tp];
              if (m) {
                if (!secMap['_IC']) secMap['_IC'] = { ingreso:0, egreso:0 };
                secMap['_IC'].ingreso += m.ingreso||0;
                secMap['_IC'].egreso  += m.egreso ||0;
              }
            }
          });
        }
      });
    });
    // Also aggregate IC from API intercompany_por_periodo (more reliable than category extraction)
    let icIngAPI = 0, icEgrAPI = 0;
    results.forEach(r => {
      if (!r?.intercompany_por_periodo) return;
      const tps = r.periodos ? (state.mes > 0
        ? [r.periodos.find(p => p === `${state.anio}-${String(state.mes).padStart(2,'0')}`)]
        : r.periodos.filter(p => p.startsWith(String(state.anio))))
        : [];
      tps.filter(Boolean).forEach(tp => {
        const m = r.intercompany_por_periodo[tp];
        if (m) { icIngAPI += m.ingreso||0; icEgrAPI += m.egreso||0; }
      });
    });
    // Use API-computed IC if available, fall back to category extraction
    const icOverride = (icIngAPI > 0 || icEgrAPI > 0)
      ? { ingreso: icIngAPI, egreso: icEgrAPI }
      : (secMap['_IC'] ? { ingreso: secMap['_IC'].ingreso, egreso: secMap['_IC'].egreso } : null);
    delete secMap['_IC'];
    const neto = totalIng - totalEgr;
    const filas = Object.entries(secMap).map(([sec, t]) => {
      const isFinanc = sec.toUpperCase().includes('FINANCIAMIENTO');
      if (isFinanc) return { seccion: sec, ingreso: t.ingreso, egreso: t.egreso, neto: t.ingreso - t.egreso };
      const netoSec = t.ingreso - t.egreso;
      if (netoSec >= 0) return { seccion: sec, ingreso: netoSec, egreso: 0, neto: netoSec };
      return { seccion: sec, ingreso: 0, egreso: Math.abs(netoSec), neto: netoSec };
    });
    const netTotalIng = filas.reduce((s,f) => s + f.ingreso, 0);
    const netTotalEgr = filas.reduce((s,f) => s + f.egreso, 0);
    setText('flSaldoIni', fmtQM(totalIni));
    setText('flIng', fmtQM(netTotalIng));
    setText('flEgr', fmtQM(netTotalEgr));
    setText('flSaldoFin', fmtQM(totalFin));
    setText('flNeto', `Neto del período: ${neto>=0?'+':''}${fmtQM(neto)}`);
    setText('flujoSub', `CONSOLIDADO · ${periodoFormal()}`);
    const filasProc = procesarFilasFlujos(filas, [], icOverride);
    setHTML('flujosTbody', [
      ...filasProc.map(f => {
        const _sl = (f.seccion === 'EGRESOS / MOVIMIENTO DE TIERRAS') ? 'EGRESOS / MOV. TIERRAS / MAQUINARIA' : f.seccion;
        return `<tr>
        <td class="bold">${_sl}</td>
        <td class="right" style="color:var(--green)">${f.ingreso > 0 ? fmtQ(f.ingreso) : '—'}</td>
        <td class="right" style="color:var(--red)">${f.egreso > 0 ? fmtQ(f.egreso) : '—'}</td>
        <td class="right bold" style="color:${f.neto>=0?'var(--green)':'var(--red)'}">${fmtQ(f.neto)}</td>
      </tr>`;
      }),
      `<tr class="total"><td>TOTAL</td><td class="right">${fmtQ(netTotalIng)}</td><td class="right">${fmtQ(netTotalEgr)}</td><td class="right">${fmtQ(neto)}</td></tr>`
    ].join(''));
    return;
  }

  // Always use mes granularity to correctly accumulate year totals
  const r = await apiFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(socSel)}&granularidad=mes`);
  state.data.flujos = r;

  if (!r || !r.periodos || !r.periodos.length) {
    setText('flSaldoIni','—'); setText('flIng','—'); setText('flEgr','—'); setText('flSaldoFin','—');
    setHTML('flujosTbody', `<tr><td colspan="4" style="text-align:center;color:var(--muted);padding:40px">Sin datos de flujos para ${state.sociedad}</td></tr>`);
    setText('flujoSub', `Sin datos disponibles para ${state.sociedad}`);
    return;
  }

  const periodos = r.periodos;

  // For "Todo el año": accumulate ALL months of the selected year
  // For specific month: use that month only
  let targetPeriodos;
  if (state.mes > 0) {
    const candidate = `${state.anio}-${String(state.mes).padStart(2,'0')}`;
    targetPeriodos = periodos.includes(candidate) ? [candidate] : [periodos[periodos.length - 1]];
  } else {
    // All months of selected year
    targetPeriodos = periodos.filter(p => p.startsWith(String(state.anio)));
    if (!targetPeriodos.length) targetPeriodos = [periodos[periodos.length - 1]];
  }

  // Saldo inicial = from first month of the selection
  const firstTarget = targetPeriodos[0];
  const lastTarget  = targetPeriodos[targetPeriodos.length - 1];
  const saldoIni = r.saldos_iniciales[firstTarget] || 0;
  const saldoFin = r.saldos_finales[lastTarget] || 0;

  let totalIng = 0, totalEgr = 0;
  const secAccum = {}; // accumulate across all targetPeriodos
  for (const sec of (r.secciones || [])) {
    for (const tp of targetPeriodos) {
      const t = sec.totales[tp];
      if (!t) continue;
      totalIng += t.ingreso || 0;
      totalEgr += t.egreso  || 0;
      if (!secAccum[sec.seccion]) secAccum[sec.seccion] = { ingreso:0, egreso:0 };
      secAccum[sec.seccion].ingreso += t.ingreso || 0;
      secAccum[sec.seccion].egreso  += t.egreso  || 0;
    }
  }
  const filasRaw = Object.entries(secAccum).map(([seccion, t]) => ({
    seccion, ingreso: t.ingreso, egreso: t.egreso, neto: t.ingreso - t.egreso
  }));
  const neto = totalIng - totalEgr;

  // Apply netting (except FINANCIAMIENTO)
  const filas = filasRaw.map(f => {
    const isFinanc = f.seccion.toUpperCase().includes('FINANCIAMIENTO');
    if (isFinanc) return f;
    const n = f.ingreso - f.egreso;
    if (n >= 0) return { ...f, ingreso: n, egreso: 0, neto: n };
    return { ...f, ingreso: 0, egreso: Math.abs(n), neto: n };
  });
  const netTotalIng = filas.reduce((s,f) => s + f.ingreso, 0);
  const netTotalEgr = filas.reduce((s,f) => s + f.egreso, 0);

  setText('flSaldoIni', fmtQM(saldoIni));
  setText('flIng', fmtQM(netTotalIng));
  setText('flEgr', fmtQM(netTotalEgr));
  setText('flSaldoFin', fmtQM(saldoFin));
  setText('flNeto', `Neto del período: ${neto >= 0 ? '+' : ''}${fmtQM(neto)}`);
  const hoyStr = new Date().toLocaleDateString('es-GT', {day:'2-digit', month:'short', year:'numeric'});
  const periodoLabel = state.mes > 0 ? lastTarget : `${state.anio} · a ${hoyStr}`;
  setText('flujoSub', `${socSel} · ${periodoLabel}`);

  // Compute IC totals from API's intercompany_por_periodo field
  const icPorPeriodo = r?.intercompany_por_periodo || {};
  let icIngNC = 0, icEgrNC = 0;
  for (const tp of targetPeriodos) {
    const m = icPorPeriodo[tp];
    if (m) { icIngNC += m.ingreso||0; icEgrNC += m.egreso||0; }
  }
  const icOverrideNC = (icIngNC > 0 || icEgrNC > 0) ? { ingreso: icIngNC, egreso: icEgrNC } : null;
  const filasProcNC = procesarFilasFlujos(filas, r?.secciones || [], icOverrideNC);
  setHTML('flujosTbody', [
    ...filasProcNC.map(f => {
        const _slnc = (f.seccion === 'EGRESOS / MOVIMIENTO DE TIERRAS') ? 'EGRESOS / MOV. TIERRAS / MAQUINARIA' : f.seccion;
        return `<tr>
      <td class="bold">${_slnc}</td>
      <td class="right" style="color:var(--green)">${f.ingreso > 0 ? fmtQ(f.ingreso) : '—'}</td>
      <td class="right" style="color:var(--red)">${f.egreso > 0 ? fmtQ(f.egreso) : '—'}</td>
      <td class="right bold" style="color:${f.neto>=0?'var(--green)':'var(--red)'}">${fmtQ(f.neto)}</td>
    </tr>`;
      }),
    `<tr class="total"><td>TOTAL</td><td class="right">${fmtQ(netTotalIng)}</td><td class="right">${fmtQ(netTotalEgr)}</td><td class="right">${fmtQ(neto)}</td></tr>`
  ].join(''));
}

/* ── Minutas ─────────────────────────────────────── */
async function loadMinutas() {
  const mes = state.mes || new Date().getMonth() + 1;
  const anio = state.anio;
  const mesKey = `rv4_minutas_${anio}_${String(mes).padStart(2,'0')}`;
  setText('minutasPeriodoSub', `Minuta de reunión · ${periodoFormal()}`);

  let items = [];
  let lastMod = '';

  // Read from localStorage (same key as minuta.html editor)
  try {
    const raw = localStorage.getItem(mesKey);
    if (raw) {
      const parsed = JSON.parse(raw);
      items = parsed.items || [];
      lastMod = parsed.lastMod || '';
    }
  } catch(e) {}

  if (items.length) {
    setHTML('minutasTbody', items.map((item, i) => `<tr>
      <td style="font-weight:${i===0?700:400};padding:10px 14px">${item.tema || item.acuerdo || '—'}</td>
      <td style="padding:10px 14px">${item.responsable || '—'}</td>
      <td style="font-size:13px;padding:10px 14px">${item.observacion || item.observación || '—'}</td>
      <td style="font-size:12px;padding:10px 14px;white-space:nowrap">${item.fecha || '—'}</td>
    </tr>`).join(''));
  } else {
    setHTML('minutasTbody', `<tr><td colspan="4" style="text-align:center;color:var(--muted);padding:40px">Sin acuerdos registrados para ${periodoFormal()} — completá la minuta en la sección Junta Directiva del tablero</td></tr>`);
  }
  if (lastMod) setText('minutasLastMod', `Última modificación: ${lastMod}`);
}
async function loadPCV() {
  const qs = periodParams();
  const [k, reg] = await Promise.all([
    apiFetch(`/api/ventas/pcv/kpis?${qs}`).catch(()=>null),
    apiFetch(`/api/ventas/registros-revision?${qs}`).catch(()=>null)
  ]);

  // The first registros-revision endpoint (line 195 in ventas.py) returns a FLAT ARRAY
  // The second one (line 1052) returns {total, rojas, amarillas, issues:[]}
  // FastAPI uses the FIRST registered route, so we get the array format.
  // Normalize both formats into the object format the presentation expects.
  let regData = reg;
  if (Array.isArray(reg)) {
    // Convert flat array → object with classified issues
    const issues = reg.map(r => {
      const issue = r.issue || '';
      let nivel = 'GRIS';
      let tipo = 'OTRO';
      if (issue.includes('sin asesor') || issue.includes('Sin código')) { nivel = 'ROJO'; tipo = 'VENTA_SIN_ASESOR'; }
      else if (issue.includes('Crédito con intereses') || issue.includes('intereses = 0')) { nivel = 'ROJO'; tipo = 'CON_INTERES_SIN_MONTO'; }
      else if (issue.includes('Caso especial')) { nivel = 'GRIS'; tipo = 'CASO_ESPECIAL'; }
      else if (issue.includes('Final Proyecto') || issue.includes('Venta Interna')) { nivel = 'AMARILLO'; tipo = 'VENTA_ESPECIAL'; }
      return {
        nivel, tipo,
        mensaje: `${issue}: ${r.cliente || r.unidad_key || '—'}`,
        detalle: `${r.nombre_proyecto || ''} | ${r.unidad_key || ''} | ${r.manzana || ''}${r.precio_final ? ' | Q ' + Number(r.precio_final).toLocaleString('es-GT') : ''}`,
        accion: nivel === 'ROJO' ? 'Corregir en SAP' : nivel === 'AMARILLO' ? 'Revisar caso' : 'Informativo'
      };
    });
    regData = {
      total: issues.length,
      rojas: issues.filter(i => i.nivel === 'ROJO').length,
      amarillas: issues.filter(i => i.nivel === 'AMARILLO').length,
      grises: issues.filter(i => i.nivel === 'GRIS').length,
      issues
    };
  }
  state.data.pcv = { k, reg: regData };

  if (k) {
    setText('pcvTotal', fmtNum(k.total_ventas));
    setText('pcvCon', fmtNum(k.con_pcv));
    setText('pcvPct', `${fmtPct(k.pct_cumplimiento)} de cumplimiento`);
    setText('pcvSin', fmtNum(k.sin_pcv));
    setText('pcvSin2026', '57 Sin PCV firmado a la fecha');
    setText('pcvDias', `${Number(k.dias_prom_gestion||0).toFixed(0)}`);
    setText('pcv0', fmtNum(k.sin_pcv_0_15));
    setText('pcv15', fmtNum(k.sin_pcv_16_30));
    setText('pcv30', fmtNum(k.sin_pcv_31_90));
    setText('pcv90', fmtNum(k.sin_pcv_mas90));
  }

  if (regData) {
    // Derive counts from issues array if the explicit fields are 0 or missing
    const issues = regData.issues || [];
    const rojas = regData.rojas || issues.filter(i => i.nivel === 'ROJO').length;
    const amarillas = regData.amarillas || issues.filter(i => i.nivel === 'AMARILLO').length;
    const grises = regData.grises || issues.filter(i => i.nivel === 'GRIS').length;
    const total = regData.total || issues.length;
    setText('regRojas', fmtNum(rojas));
    setText('regAmar', fmtNum(amarillas));
    setText('regGris', fmtNum(grises));
    setText('regTotal', fmtNum(total));
    const top = issues;
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
  setText('rsmSinPCV', '57');
  setText('rsmPCVPct', '');
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

function drawTendencia(data, metaVal) {
  const container = document.getElementById('tendenciaChart');
  if (!container || !data || !data.length) return;
  const W = 1600, H = 480, pad = { l: 60, r: 30, t: 30, b: 60 };
  const maxVal = Math.max(...data.map(d => Math.max(Number(d.ventas_brutas||0), Number(d.desistimientos||0))), 1);
  const max = maxVal;
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

  // Meta reference line (horizontal dashed)
  let metaLine = '';
  if (metaVal > 0) {
    const yMeta = y(metaVal);
    metaLine = `<line x1="${pad.l}" y1="${yMeta}" x2="${W - pad.r}" y2="${yMeta}" stroke="var(--dorado)" stroke-width="2" stroke-dasharray="8,5" opacity="0.8"/>` +
               `<text x="${W - pad.r + 4}" y="${yMeta + 4}" font-size="12" font-weight="700" fill="var(--dorado)" style="font-family:Montserrat">Meta ${metaVal}</text>`;
  }

  const svgContent = grid + xLabels + metaLine +
    linePath('ventas_brutas', 'var(--blue)') +
    linePath('ventas_netas',  'var(--green)') +
    data.map((d,i) => {
      const yy = y(Number(d.desistimientos||0));
      const barH = Math.max(0, H-pad.b-yy);
      return barH > 0 ? `<rect x="${x(i)-12}" y="${yy}" width="24" height="${barH}" fill="var(--red)" opacity="0.6" rx="2"/>` : '';
    }).join('') +
    data.map((d,i) => {
      let labels = `<text x="${x(i)}" y="${y(Number(d.ventas_brutas||0))-14}" text-anchor="middle" font-size="13" font-weight="700" fill="var(--blue)" style="font-family:Montserrat">${d.ventas_brutas||0}</text>`;
      labels += `<text x="${x(i)}" y="${y(Number(d.ventas_netas||0))+22}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--green)" style="font-family:Montserrat">${d.ventas_netas||0}</text>`;
      const des = Number(d.desistimientos||0);
      if (des > 0) labels += `<text x="${x(i)}" y="${y(des)-6}" text-anchor="middle" font-size="11" font-weight="700" fill="var(--red)" style="font-family:Montserrat">${des}</text>`;
      return labels;
    }).join('');

  // Replace container content with a fresh SVG element
  container.innerHTML = `<svg viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:${H}px">${svgContent}</svg>`;
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
        { rango:'91-120 días',cuotas:165, clientes:80,  monto:5600000 },
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
  // Mark current version to detect stale responses
  const myVersion = ++window._loadAllVersion;
  const isStale = () => myVersion !== window._loadAllVersion;
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
    renderVentasAnio();
    renderCXP();
    renderInteresesSinCobrar();
    renderVentasPorPlazo();
    updateAllPeriodLabels();
    return;
  }

  // Live data
  try {
    await Promise.all([loadInventario(), loadVentas(), loadCartera(), loadFlujos(), loadDetalleFlujos(), loadPCV()]);
    loadMinutas(); // fire-and-forget (non-critical)
    renderResumenEjecutivo();
    renderVentasAnio();
    renderCXP();
    renderInteresesSinCobrar();
    renderVentasPorPlazo();
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
  // "Valor venta al contado" subtitle below disponibles value
  const invDispValEl = document.getElementById('invDispVal');
  if (invDispValEl) {
    const existing = document.getElementById('invDispSubLabel');
    if (!existing) {
      const sub = document.createElement('div');
      sub.id = 'invDispSubLabel';
      sub.style.cssText = 'font-size:10px;color:var(--muted);font-weight:600;letter-spacing:.05em;margin-top:2px';
      sub.textContent = 'Valor venta al contado';
      invDispValEl.parentNode.insertBefore(sub, invDispValEl.nextSibling);
    }
  }
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
    // Compute intereses sin cobrar (oportunidad) using new formula:
    // (contado + sin_interés) × ticket_promedio × 10% anual × (plazo_promedio / 12)
    const udsNoInt    = (k.contado||0) + (k.sin_interes||0);
    const montoNoInt  = udsNoInt * (k.ticket_promedio||0);
    const plazoAnios  = (k.plazo_promedio||0) / 12;
    window._oportunidadCalculada = Math.round(montoNoInt * 0.10 * plazoAnios);
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
          <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--green);width:${pctOf(fin.intereses_cobrados,fin.intereses_cobrados+(window._oportunidadCalculada||fin.intereses_no_cobrados||0))}%"></div></div>
        </div>
        <div><div style="display:flex;justify-content:space-between;font-size:13px;font-weight:600;color:var(--red);margin-bottom:6px"><span>Intereses no cobrados</span><span>${fmtQ((window._oportunidadCalculada||fin.intereses_no_cobrados||0))}</span></div>
          <div style="height:14px;background:var(--border-soft);border-radius:4px;overflow:hidden"><div style="height:100%;background:var(--red);width:${pctOf((window._oportunidadCalculada||fin.intereses_no_cobrados||0),fin.intereses_cobrados+(window._oportunidadCalculada||fin.intereses_no_cobrados||0))}%"></div></div>
        </div>
        <div style="padding-top:10px;border-top:1px solid var(--border);font-size:13px;color:var(--muted);font-weight:500">Tasa anual implícita: <strong style="color:var(--dorado)">${fmtPct(fin.tasa_anual_implicita)}</strong> · Captura: <strong>${fmtPct(fin.ratio_cobrado_vs_oportunidad)}</strong></div>
      </div>`);

    setText('finTasa', fmtPct(fin.tasa_anual_implicita));
    setText('finCobrados', fmtQ(fin.intereses_cobrados));  // label updated in HTML to 'Intereses x Cobrar'
    setText('finCobLotes', `${fmtNum(fin.lotes_con_int)} contratos con interés`);
    setText('finNoCobrados', fmtQ((window._oportunidadCalculada||fin.intereses_no_cobrados||0)));
    setText('finNoCobLotes', `${fmtNum(fin.lotes_sin_int)} contratos sin interés`);
    setText('finLectura', `La tasa implícita anual de los contratos con interés es ${fmtPct(fin.tasa_anual_implicita)}. De aplicarla a los contratos sin interés, generaría ${fmtQM((window._oportunidadCalculada||fin.intereses_no_cobrados||0))} adicionales — la captura actual es del ${fmtPct(fin.ratio_cobrado_vs_oportunidad)}.`);

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
    // Determine months to show based on filter
    const metasMeses = state.mes > 0
      ? [state.mes]
      : [1,2,3,4,5,6,7,8,9,10,11,12].filter(m => m <= new Date().getMonth()+1);
    const mesNombres = ['','Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];

    // Build project rows: use API data + fixed metas
    // Group metas by display project
    const metasByProy = {};
    v.metas.forEach(m => {
      const rawName = m.proyecto || m.responsable || '';
      // Find display name
      let displayName = rawName;
      for (const [k,v2] of Object.entries(PROYECTO_DISPLAY_MAP)) {
        if (rawName.toLowerCase().includes(k.toLowerCase()) || k.toLowerCase().includes(rawName.toLowerCase())) {
          displayName = v2; break;
        }
      }
      if (!metasByProy[displayName]) metasByProy[displayName] = { ventas: 0, meses: {} };
      metasByProy[displayName].ventas += Number(m.ventas_total||0);
      // per-mes data if available
      if (m.por_mes) {
        Object.entries(m.por_mes).forEach(([mes, val]) => {
          metasByProy[displayName].meses[mes] = (metasByProy[displayName].meses[mes]||0) + Number(val||0);
        });
      }
    });

    // Add projects with metas but no sales
    Object.keys(METAS_POR_PROYECTO).forEach(p => {
      if (!metasByProy[p]) metasByProy[p] = { ventas: 0, meses: {} };
    });

    const colsHeader = metasMeses.map(m => `<th style="padding:5px 8px;text-align:center;font-size:10px;color:var(--muted)">${mesNombres[m]}</th>`).join('');
    const totalMeta = Object.values(METAS_POR_PROYECTO).reduce((s,v)=>s+v,0);
    let totalReal = 0;

    const rows = Object.entries(metasByProy).sort((a,b)=>{
      const ma = METAS_POR_PROYECTO[a[0]]||0, mb = METAS_POR_PROYECTO[b[0]]||0;
      return mb - ma;
    }).map(([proy, data]) => {
      const metaMes = METAS_POR_PROYECTO[proy] || 0;
      const metaTotal = state.mes > 0 ? metaMes : metaMes * metasMeses.length;
      const realTotal = data.ventas;
      totalReal += realTotal;
      const pct = metaTotal > 0 ? Math.round(realTotal/metaTotal*100) : 0;
      const color = pct>=80?'var(--green)':pct>=50?'var(--dorado)':'var(--red)';
      const mesCells = metasMeses.map(m => {
        const realMes = data.meses[m] || 0;
        const rColor = metaMes > 0 ? (realMes >= metaMes ? 'var(--green)' : 'var(--red)') : 'var(--muted)';
        return `<td style="padding:4px 8px;text-align:center;font-size:10px">
          <div style="color:var(--muted);font-size:9px">Meta: ${metaMes}</div>
          <div style="color:${rColor};font-weight:700">${realMes||0}</div>
        </td>`;
      }).join('');
      const barW = Math.min(pct, 100);
      return `<tr style="border-bottom:1px solid var(--border)">
        <td style="padding:6px 10px;font-size:11px;font-weight:600;white-space:nowrap;min-width:200px">${proy}</td>
        ${mesCells}
        <td style="padding:6px 8px;text-align:center;font-weight:700;font-size:12px;color:var(--dorado)">${metaTotal}</td>
        <td style="padding:6px 8px;text-align:center;font-weight:700;font-size:12px">${realTotal}</td>
        <td style="padding:6px 8px;min-width:100px">
          <div style="height:8px;background:rgba(255,255,255,.08);border-radius:4px;overflow:hidden">
            <div style="height:100%;background:${color};width:${barW}%"></div>
          </div>
          <div style="font-size:10px;font-weight:700;color:${color};text-align:right;margin-top:2px">${pct}%</div>
        </td>
      </tr>`;
    }).join('');

    const totalMetaConsol = state.mes > 0 ? totalMeta : totalMeta * metasMeses.length;
    const totalPct = totalMetaConsol > 0 ? Math.round(totalReal/totalMetaConsol*100) : 0;
    const totColor = totalPct>=80?'var(--green)':totalPct>=50?'var(--dorado)':'var(--red)';

    setHTML('metasRows', `<table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:var(--azul-marino,#0d2340)">
        <th style="padding:6px 10px;text-align:left;font-size:11px;color:#fff;white-space:nowrap">Sociedad / Proyecto</th>
        ${colsHeader}
        <th style="padding:6px 8px;text-align:center;font-size:10px;color:var(--dorado)">Meta</th>
        <th style="padding:6px 8px;text-align:center;font-size:10px;color:var(--blue)">Real</th>
        <th style="padding:6px 8px;text-align:center;font-size:10px;color:var(--muted)">Avance</th>
      </tr></thead>
      <tbody>${rows}</tbody>
      <tfoot><tr style="background:var(--bg-section);font-weight:700">
        <td style="padding:6px 10px;font-size:11px">TOTAL</td>
        ${metasMeses.map(()=>'<td></td>').join('')}
        <td style="padding:6px 8px;text-align:center;color:var(--dorado)">${totalMetaConsol}</td>
        <td style="padding:6px 8px;text-align:center;color:var(--blue)">${totalReal}</td>
        <td style="padding:6px 8px"><div style="height:8px;background:rgba(255,255,255,.08);border-radius:4px;overflow:hidden"><div style="height:100%;background:${totColor};width:${Math.min(totalPct,100)}%"></div></div><div style="font-size:10px;font-weight:700;color:${totColor};text-align:right;margin-top:2px">${totalPct}%</div></td>
      </tr></tfoot>
    </table>`);
  }

  if (v.tend && v.tend.length) {
    drawTendencia(v.tend);
  } else {
    const el = document.getElementById('tendenciaChart');
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:300px;color:var(--muted);font-size:14px">Sin datos de ventas para el período seleccionado</div>';
  }
}

/* ── Slide 11 — Intereses sin cobrar por año (tabla fija, sin filtro fecha) ── */
async function renderInteresesSinCobrar() {
  const container = document.getElementById('interesesSinCobrarContainer');
  if (!container) return;
  // Fixed table (2024-2026): compute oportunidad using ventas/kpis per year
  // Formula: (contado + sin_interes) × ticket_promedio × 10% × (plazo_promedio / 12)
  try {
    const years = [2024, 2025, 2026];
    const kpis = await Promise.all(
      years.map(yr => apiFetch(`/api/ventas/kpis?año=${yr}`))
    );
    const rows = kpis.map((k, i) => {
      if (!k) return null;
      const yr = years[i];
      const udsNoInt   = (k.contado||0) + (k.sin_interes||0);
      const montoNoInt = udsNoInt * (k.ticket_promedio||0);
      const plazoAnios = (k.plazo_promedio||0) / 12;
      const oport      = Math.round(montoNoInt * 0.10 * plazoAnios);
      return { anio: yr, uds: udsNoInt, monto: montoNoInt, plazo_m: k.plazo_promedio||0, oportunidad: oport };
    }).filter(Boolean);

    if (!rows.length) {
      container.innerHTML = '<div style="color:var(--muted);font-size:11px;padding:8px">Sin datos</div>';
      return;
    }

    const total = rows.reduce((s,r) => s + r.oportunidad, 0);
    const data = { por_anio: rows, total };
    // data.por_anio has: { anio, uds, monto, plazo_m, oportunidad }
    const rowsHtml = data.por_anio.map(r => {
      const pct = total > 0 ? (r.oportunidad/total*100).toFixed(1) : '0.0';
      return `<tr>
        <td style="padding:5px 8px;font-weight:700;color:var(--dorado)">${r.anio}</td>
        <td style="padding:5px 8px;text-align:right">${fmtNum(r.uds)}</td>
        <td style="padding:5px 8px;text-align:right;font-size:10px">${fmtQM(r.monto)}</td>
        <td style="padding:5px 8px;text-align:right;font-size:10px">${Math.round(r.plazo_m)}m</td>
        <td style="padding:5px 8px;text-align:right;font-weight:700;color:var(--red)">${fmtQ(r.oportunidad)}</td>
        <td style="padding:5px 8px;text-align:right;color:var(--muted)">${pct}%</td>
      </tr>`;
    }).join('');
    container.innerHTML = `
      <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">
        Intereses sin cobrar — Oportunidad (tabla fija 2024-2026)
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:11px">
        <thead><tr style="background:var(--bg-section)">
          <th style="padding:4px 8px;text-align:left;font-size:10px;color:var(--muted)">Año</th>
          <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">Uds s/int</th>
          <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">Monto venta</th>
          <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">Plazo prom.</th>
          <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">Int. oport. (10%)</th>
          <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">%</th>
        </tr></thead>
        <tbody>${rowsHtml}</tbody>
        <tfoot><tr style="background:var(--bg-section);font-weight:700;border-top:2px solid var(--border)">
          <td style="padding:5px 8px" colspan="4">TOTAL</td>
          <td style="padding:5px 8px;text-align:right;color:var(--dorado)">${fmtQ(total)}</td>
          <td style="padding:5px 8px;text-align:right">100%</td>
        </tr></tfoot>
      </table>
      <div style="font-size:9px;color:var(--muted);margin-top:4px">
        Fórmula: (Contado + Sin interés) × Ticket prom. × 10% × (Plazo prom. / 12)
      </div>`;
  } catch(e) {
    container.innerHTML = `<div style="color:var(--red);font-size:11px;padding:8px">Error: ${e.message}</div>`;
  }
}

/* ── Slide 11 — Ventas activas por grupo de plazo × año (tabla fija) ── */
async function renderVentasPorPlazo() {
  const container = document.getElementById('ventasPorPlazoContainer');
  if (!container) return;
  try {
    let data = await apiFetch('/api/ventas/por-plazo-historico');
    // If API not available, show empty table structure
    if (!data || !data.grupos) {
      container.innerHTML = '<div style="font-size:10px;color:var(--muted);padding:8px">Sin datos · endpoint pendiente</div>';
      return;
    }
    const { grupos, years, totales_anio, total_general } = data;
    const colsHtml = years.map(y => `<th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--dorado);white-space:nowrap">${y}</th>`).join('');
    const rowsHtml = grupos.map(g => {
      const acum = Object.values(g.por_anio).reduce((s,v)=>s+v,0);
      const pct  = total_general > 0 ? (acum/total_general*100).toFixed(1) : '0.0';
      return `<tr>
        <td style="padding:4px 8px;font-weight:600;font-size:11px;white-space:nowrap">${g.grupo}</td>
        ${years.map(y => { const v = g.por_anio[y]||0; return `<td style="padding:4px 8px;text-align:right;font-size:11px;font-weight:${v>0?700:400};color:${v>0?'var(--text)':'var(--muted)'}">${v||'—'}</td>`; }).join('')}
        <td style="padding:4px 8px;text-align:right;font-weight:700;color:var(--dorado);font-size:11px">${acum}</td>
        <td style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">${pct}%</td>
      </tr>`;
    }).join('');
    container.innerHTML = `
      <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px">Ventas activas por plazo</div>
      <div style="overflow:auto">
        <table style="width:100%;border-collapse:collapse;font-size:11px">
          <thead><tr style="background:var(--bg-section)">
            <th style="padding:4px 8px;text-align:left;font-size:10px;color:var(--muted)">Grupo Plazo</th>
            ${colsHtml}
            <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--dorado)">Acumulado</th>
            <th style="padding:4px 8px;text-align:right;font-size:10px;color:var(--muted)">%</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
          <tfoot><tr style="background:var(--bg-section);font-weight:700;border-top:2px solid var(--border)">
            <td style="padding:5px 8px">Total año</td>
            ${years.map(y=>`<td style="padding:5px 8px;text-align:right;color:var(--dorado)">${totales_anio[y]||'—'}</td>`).join('')}
            <td style="padding:5px 8px;text-align:right;color:var(--dorado)">${total_general}</td>
            <td style="padding:5px 8px;text-align:right">100%</td>
          </tr></tfoot>
        </table>
      </div>`;
  } catch(e) {
    if (container) container.innerHTML = `<div style="color:var(--red);font-size:11px;padding:8px">Error: ${e.message}</div>`;
  }
}




async function loadCartera() {
  const cqs = carteraPeriodParams();
  try {
    const [k, aging, proy, alertas, desist, morosos] = await Promise.all([
      apiFetch(`/api/cartera/kpis?${cqs}`),
      apiFetch(`/api/cartera/aging?${cqs}`),
      apiFetch(`/api/cartera/proyeccion-mensual?meses=12&${cqs}`),
      apiFetch('/api/cartera/alertas').catch(()=>null),
      apiFetch(`/api/cartera/desistimientos?page_size=50${state.mes>0?'&mes='+state.mes+'&año='+state.anio:''}`).catch(()=>null),
      apiFetch('/api/cartera/morosos?dias_min=61').catch(()=>null),
    ]);
    state.data.cartera = { k, aging, proy, alertas, desist, morosos };
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
    setText('desLectura', `En ${periodoFormal()}, ${fmtNum(k.desistimientos_total)} desistimientos representaron ${fmtQM(k.desistimientos_pagado)} en pagos de clientes. Se reintegró ${fmtQM(k.desistimientos_reintegrado)} y la sociedad retuvo ${fmtQM(ret)} por concepto de penalizaciones contractuales.`);
  }
  if (c.aging) {
    const agingTotal = c.aging.reduce((s,a)=>s+Number(a.monto||0),0);
    const agingRows = c.aging.map(a=>{
      const es0_30 = a.rango && a.rango.includes('0-30');
      const pct = agingTotal > 0 ? (Number(a.monto||0)/agingTotal*100).toFixed(1)+'%' : '—';
      const color = es0_30 ? 'var(--green)' : 'var(--red)';
      return `<tr><td class="bold">${a.rango}</td><td class="right">${fmtNum(a.cuotas)}</td><td class="right">${fmtNum(a.clientes)}</td><td class="right bold" style="color:${color}">${fmtQ(a.monto)}</td><td class="right" style="color:${color}">${pct}</td></tr>`;
    });
    const agingTotCuotas = c.aging.reduce((s,a)=>s+(Number(a.cuotas)||0),0);
    const agingTotClientes = c.aging.reduce((s,a)=>s+(Number(a.clientes)||0),0);
    agingRows.push(`<tr style="background:var(--bg-section);font-weight:700"><td class="bold">TOTAL</td><td class="right">${fmtNum(agingTotCuotas)}</td><td class="right">${fmtNum(agingTotClientes)}</td><td class="right bold" style="color:var(--amber)">${fmtQ(agingTotal)}</td><td class="right" style="color:var(--amber)">100%</td></tr>`);
    setHTML('agingTbody', agingRows.join(''));
    drawAging(c.aging);
    const total = c.aging.reduce((s,a)=>s+Number(a.monto||0),0);
    // Rangos con más de 60 días — usar rangos REALES (61-90 días, 91-120 días)
    const criticos = c.aging.filter(a => a.rango && (
      a.rango.includes('61-90') ||
      a.rango.includes('91-120') ||
      a.rango.includes('+120') ||
      a.rango.includes('>120')
    ));
    const monto = criticos.reduce((s,a)=>s+Number(a.monto||0),0);
    const pctCritico = agingTotal > 0 ? (monto/agingTotal*100) : 0;
    // Conteo de clientes en los rangos críticos (no de filas, de clientes únicos en esos rangos)
    const clientesCriticos = criticos.reduce((s,a)=>s+Number(a.clientes||0),0);
    const lectura = pctCritico > 0
      ? `${fmtPct(pctCritico)} del monto vencido (${fmtQM(monto)}) tiene más de 60 días de mora, afectando a ${fmtNum(clientesCriticos)} clientes — ${pctCritico > 30 ? 'requieren gestión inmediata o provisión.' : 'monitorear evolución y reforzar gestión de cobranza.'}`
      : `Sin clientes en rango crítico de mora (>60 días). Cartera bajo control.`;
    setText('agingLectura', lectura);
  }
  // Render desistimientos detail cards (slide 18)
  if (c.desist && c.desist.desistimientos && c.desist.desistimientos.length) {
    const rows = c.desist.desistimientos;
    setHTML('desCards', rows.map(d => {
      const ret = (Number(d.pagado_capital||0)) - (Number(d.reintegrado_cliente||0));
      const diasPlazo = d.fecha_venta && d.fecha_desistimiento
        ? Math.round((new Date(d.fecha_desistimiento)-new Date(d.fecha_venta))/(1000*60*60*24)) : null;
      return `<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;border-left:4px solid var(--red)">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
          <div style="font-size:13px;font-weight:700;color:var(--text)">${d.nombre_cliente||'—'}</div>
          <div style="font-size:11px;font-weight:600;color:var(--red)">${d.fecha_desistimiento?String(d.fecha_desistimiento).slice(0,10):'—'}</div>
        </div>
        <div style="font-size:11px;color:var(--muted);margin-bottom:8px">${d.empresa||'—'} · ${d.lote||'—'}</div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;font-size:11px">
          <div><span style="color:var(--muted)">Precio</span><div style="font-weight:700;color:var(--text)">${d.precio_con_descuento?fmtQ(d.precio_con_descuento):'—'}</div></div>
          <div><span style="color:var(--muted)">Pagado</span><div style="font-weight:700;color:var(--text)">${d.pagado_capital?fmtQ(d.pagado_capital):'—'}</div></div>
          <div><span style="color:var(--muted)">Retenido</span><div style="font-weight:700;color:var(--amber)">${ret>0?fmtQ(ret):'Q 0'}</div></div>
        </div>
        ${d.motivo_desistimiento?`<div style="margin-top:8px;font-size:10px;color:var(--muted);border-top:1px solid var(--border-soft);padding-top:6px">${d.motivo_desistimiento}</div>`:''}
      </div>`;
    }).join(''));
  } else {
    setHTML('desCards', '<div style="grid-column:1/-1;text-align:center;color:var(--muted);padding:30px;font-size:14px">Sin desistimientos en el período</div>');
  }

  if (c.proy && c.proy.length) drawProyeccion(c.proy);

  // Morosos 61+ días (new slide)
  if (c.morosos && c.morosos.clientes) {
    const clientes = c.morosos.clientes;
    const totalMonto = clientes.reduce((s,m) => s + Number(m.monto_vencido||0), 0);
    const avgDias = clientes.length ? Math.round(clientes.reduce((s,m) => s + Number(m.dias_mora||0), 0) / clientes.length) : 0;
    setText('morTotal', fmtNum(clientes.length));
    setText('morMonto', fmtQM(totalMonto));
    setText('morDiasAvg', `${avgDias}d`);
    setHTML('morTbody', clientes.map(m => {
      const rangoClass = (m.dias_mora||0) > 90 ? 'red' : (m.dias_mora||0) > 60 ? 'amber' : '';
      return `<tr>
        <td class="bold" style="max-width:220px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${m.card_name||'—'}</td>
        <td>${m.empresa||'—'}</td>
        <td class="right">${fmtNum(m.cuotas_vencidas)}</td>
        <td class="right bold" style="color:var(--red)">${fmtQ(m.monto_vencido)}</td>
        <td class="right bold">${m.dias_mora||'—'}d</td>
        <td><span class="pill ${rangoClass}">${(m.rango_mora||'—').replace('91-180 días','91-120 días')}</span></td>
      </tr>`;
    }).join('') || '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:30px">Sin clientes morosos 61+ días</td></tr>');
  }

  if (c.alertas) {
    setText('alRoja', fmtNum(c.alertas.rojas));
    setText('alAmar', fmtNum(c.alertas.amarillas));
    setText('alTotal', fmtNum(c.alertas.total));
    const top = (c.alertas.alertas||[]);
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
    setText('pcvSin2026', '57 Sin PCV firmado a la fecha');
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


/* ── Ventas por Año (histórico) ───────────────────────── */
async function renderVentasAnio() {
  const container = document.getElementById('ventasAnioContainer');
  if (!container) return;
  container.innerHTML = '<div style="color:var(--muted);text-align:center;padding:30px">Cargando...</div>';
  try {
    const data = await apiFetch('/api/ventas/historico-anios');
    if (!data || !data.anios || !data.anios.length) {
      container.innerHTML = '<div style="color:var(--muted);text-align:center;padding:40px">Sin datos históricos</div>';
      return;
    }
    const years     = data.anios.map(r => r.anio).sort();
    const yearTotals = {};
    data.anios.forEach(r => { yearTotals[r.anio] = r.brutas || 0; });
    const totalBrutas = data.anios.reduce((s,r)=>s+(r.brutas||0), 0);
    const PMAP = {"Hacienda Jumay": "Eficiencia Urbana — Hacienda Jumay", "La Ceiba": "Servicios Generales — La Ceiba", "Hacienda el Sol": "Rossio — Hacienda el Sol", "Oasis Zacapa": "Frugalex — Oasis Zacapa", "Cañadas de Jalapa": "Ottavia — Cañadas de Jalapa", "Condado Jutiapa": "Utilica — Condado Jutiapa", "Club Campestre Jumay": "Tezzoli — Club Campestre Jumay", "Club del Bosque": "Urbiva — Club del Bosque", "Club Residencial Progreso": "Garbatella — Club Residencial El Progreso", "Arboleda Santa Elena": "Capipos — Arboleda Santa Elena", "Hacienda Santa Lucia": "Ovest — Hacienda Santa Lucia", "Hacienda El Cafetal Fase I": "Corcolle — Hacienda El Cafetal Fase I", "Hacienda El Cafetal Fase III": "Gibraleón — Hacienda El Cafetal Fase III"};
    const proyInv   = window.state?.data?.inventario?.proyectos || [];
    const colsHTML  = years.map(y => `<th class="right" style="color:var(--dorado)">${y}</th>`).join('');
    const rowsHTML  = proyInv
      .filter(p => (p.vendidos||0) > 0)
      .sort((a,b) => (b.vendidos||0) - (a.vendidos||0))
      .map(p => {
        const pn   = p.nombre_proyecto || '—';
        const disp = PMAP[pn] || (p.sociedad ? `${p.sociedad} — ${pn}` : pn);
        return `<tr>
          <td style="padding:6px 12px;font-weight:600;font-size:12px">${disp}</td>
          ${years.map(() => '<td class="right" style="font-size:11px;color:var(--muted)">—</td>').join('')}
          <td class="right bold" style="color:var(--dorado);font-size:12px">${p.vendidos||0}</td>
        </tr>`;
      }).join('');
    container.innerHTML = `
      <div style="overflow:auto;max-height:calc(100% - 36px)">
        <table class="exec-table" style="font-size:12px;width:100%">
          <thead><tr>
            <th style="min-width:220px">Sociedad / Proyecto</th>${colsHTML}
            <th class="right" style="color:var(--dorado)">Total vendido</th>
          </tr></thead>
          <tbody>${rowsHTML}</tbody>
          <tfoot><tr style="background:var(--bg-section);font-weight:700">
            <td style="padding:8px 12px">Total por año</td>
            ${years.map(y=>`<td class="right" style="color:var(--dorado)">${yearTotals[y]||'—'}</td>`).join('')}
            <td class="right" style="color:var(--dorado)">${totalBrutas}</td>
          </tr></tfoot>
        </table>
      </div>
      <div style="font-size:10px;color:var(--muted);padding:4px 8px;margin-top:4px;border-top:1px dashed var(--border)">
        Total vendido por proyecto: dato acumulado del inventario · Totales anuales: datos reales de ventas SAP
      </div>`;
  } catch(e) {
    if (container) container.innerHTML = `<div style="color:var(--muted);text-align:center;padding:40px">Error: ${e.message}</div>`;
  }
}





/* ── CXP Cuentas por Pagar — datos embebidos del Excel ── */
const CXP_DATA = [{"EMPRESA": "CAPIPOS", "Nº documento": 1000030, "Código de proveedor": "PL-00025", "Nombre de acreedor": "ARQUITECTURA SIETE, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "214598E1-953829669", "Fecha de vencimiento": "2026-05-20", "Importe": 25000.0, "Comentarios": "214598E1-38DA-4925-935A-8BDA2E62C212 | Procesado por RV4 APAgent | TKT 00627"}, {"EMPRESA": "CAPIPOS", "Nº documento": 1000029, "Código de proveedor": "PL-00018", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-3DD2FC41-1919305297", "Fecha de vencimiento": "2026-04-29", "Importe": 1645.0, "Comentarios": "Servicios prestados del mes de marzo 2026"}, {"EMPRESA": "CAPIPOS", "Nº documento": 1000031, "Código de proveedor": "PL-00018", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-7365DD1C-2869116991", "Fecha de vencimiento": "2026-05-15", "Importe": 1645.0, "Comentarios": "Servicios prestados del mes de abril del año 2026."}, {"EMPRESA": "CAPIPOS", "Nº documento": 1000022, "Código de proveedor": "PL-00023", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-6E34C6A7-2905882657", "Fecha de vencimiento": "2026-03-11", "Importe": 16652.16, "Comentarios": "Concreto calle de acceso"}, {"EMPRESA": "CAPIPOS", "Nº documento": 1000023, "Código de proveedor": "PL-00023", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-7D98559C-3031715781", "Fecha de vencimiento": "2026-03-11", "Importe": 32004.56, "Comentarios": "Concreto calle de acceso"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 939, "Código de proveedor": "PL-00082", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-22B38302-1096896317", "Fecha de vencimiento": "2026-04-29", "Importe": 10717.0, "Comentarios": "Servicios prestados del mes de marzo 2026"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 955, "Código de proveedor": "PL-00082", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-20092132-402411075", "Fecha de vencimiento": "2026-05-08", "Importe": 14077.0, "Comentarios": "Servicios prestados mes de abril 2026"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 951, "Código de proveedor": "PL-00145", "Nombre de acreedor": "DISTRIBUIDORA DE ELECTRICIDAD DE ORIENTE SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-916C83CF-1362643732", "Fecha de vencimiento": "2026-05-15", "Importe": 3308.02, "Comentarios": "MES DE ABRIL RECIBO DE LUZ NIS: 7245296"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 953, "Código de proveedor": "PL-00145", "Nombre de acreedor": "DISTRIBUIDORA DE ELECTRICIDAD DE ORIENTE SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-F53A2EC3-1482903022", "Fecha de vencimiento": "2026-05-15", "Importe": 56.24, "Comentarios": "MES DE ABRIL RECIBO DE LUZ NIS: 7194167"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 954, "Código de proveedor": "PL-00145", "Nombre de acreedor": "DISTRIBUIDORA DE ELECTRICIDAD DE ORIENTE SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-95CC4D9C-582960706", "Fecha de vencimiento": "2026-05-15", "Importe": 815.0, "Comentarios": "MES DE ABRIL Recibo de luz NIS: 7189639"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 160, "Código de proveedor": "PL-00035", "Nombre de acreedor": "FUERZA ELITE SEGURIDAD, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-EAF44428-1433225018", "Fecha de vencimiento": "2024-02-28", "Importe": 8502.68, "Comentarios": "seguridad del poryecto Basado en Solicitud de compra 42. Basado en Pedidos 39."}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 175, "Código de proveedor": "PL-00035", "Nombre de acreedor": "FUERZA ELITE SEGURIDAD, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-F9A00F75-3910485807", "Fecha de vencimiento": "2024-03-06", "Importe": 8502.68, "Comentarios": "Seguridad de 11 de febrero al 10 de marzo 2023 Basado en Solicitud de compra 56. Basado en Pedidos 53."}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 2, "Código de proveedor": "PL-00002", "Nombre de acreedor": "GRUPO A&C CONSTRUCTORES", "No.Ref.del acreedor": "FC-439DCAC1-2945076703", "Fecha de vencimiento": "2023-07-21", "Importe": 28660.71, "Comentarios": "Limpieza de calles Condado la Ceiba"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 259, "Código de proveedor": "PL-00002", "Nombre de acreedor": "GRUPO A&C CONSTRUCTORES", "No.Ref.del acreedor": "FC-DBD64039-4039132626", "Fecha de vencimiento": "2024-06-12", "Importe": 31001.79, "Comentarios": "200 horas de renta de retroexcavadora a Q315.00 con 3 horas minimas"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 310, "Código de proveedor": "PL-00002", "Nombre de acreedor": "GRUPO A&C CONSTRUCTORES", "No.Ref.del acreedor": "FC-7638DFA3-4225975200", "Fecha de vencimiento": "2024-07-24", "Importe": 3189.94, "Comentarios": "10.6 Horas de renta de retroexcavadora"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 311, "Código de proveedor": "PL-00002", "Nombre de acreedor": "GRUPO A&C CONSTRUCTORES", "No.Ref.del acreedor": "FC-5E1937E3-3463859669", "Fecha de vencimiento": "2024-07-24", "Importe": 21352.23, "Comentarios": "44.7 horas de renta de Patrol o Moto NIveladora"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 541, "Código de proveedor": "PL-00002", "Nombre de acreedor": "GRUPO A&C CONSTRUCTORES", "No.Ref.del acreedor": "FC-8D26F12B-3517137266", "Fecha de vencimiento": "2025-03-26", "Importe": 56850.0, "Comentarios": "Venta de cabezal"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 894, "Código de proveedor": "PL-00005", "Nombre de acreedor": "GRUPO CONSERSA, S.A.", "No.Ref.del acreedor": "FC-8153FAE3-3986180897", "Fecha de vencimiento": "2026-02-11", "Importe": 9240.0, "Comentarios": "Servicios administrativos del mes de febrero 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 932, "Código de proveedor": "PL-00005", "Nombre de acreedor": "GRUPO CONSERSA, S.A.", "No.Ref.del acreedor": "FC-AED9D547-2880783487", "Fecha de vencimiento": "2026-04-10", "Importe": 37030.0, "Comentarios": "Fee comercial mes de marzo al mes de julio 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 657, "Código de proveedor": "PL-00063", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-992A6AC8-2782087358", "Fecha de vencimiento": "2025-07-02", "Importe": 13350.0, "Comentarios": "Tapaderas y rejillas para 3RA CALLE, estas tapaderas y rejillas"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 849, "Código de proveedor": "PL-00063", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-4E2615A3-3959769782", "Fecha de vencimiento": "2026-01-07", "Importe": 1200.0, "Comentarios": "1 Flete"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 848, "Código de proveedor": "PL-00063", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-32B3D8A6-2787199185", "Fecha de vencimiento": "2026-01-14", "Importe": 10600.0, "Comentarios": "8 Tapaderas prefabricadas y 4 Rejillas prefaricadas"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 703, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-7191D5A9-1131695209", "Fecha de vencimiento": "2025-08-06", "Importe": 3553.93, "Comentarios": "12mts de arena y 12 mts de piedrin"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 733, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-9A17B8BD-3668527436", "Fecha de vencimiento": "2025-08-20", "Importe": 7107.86, "Comentarios": "24m3 de arena y 24m3 de piedrin"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 741, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-D9287277-1287605117", "Fecha de vencimiento": "2025-08-27", "Importe": 3553.93, "Comentarios": "12m3 de arena y 12m3 de piedrin"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 757, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-4D426B34-3903800505", "Fecha de vencimiento": "2025-09-10", "Importe": 8884.82, "Comentarios": "36 MTS DE PIEDRÍN y 24 MTS DE ARENA"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 799, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-1959AF56-4137764594", "Fecha de vencimiento": "2025-10-13", "Importe": 3553.93, "Comentarios": "12m3 de arena u 12m3 de piedrin"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 809, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-8B1C793C-10634905", "Fecha de vencimiento": "2025-11-05", "Importe": 3553.93, "Comentarios": "12 MTS DE ARENA y 12 MTS DE PIEDRÍN"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 820, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-D619932F-1701988597", "Fecha de vencimiento": "2025-11-19", "Importe": 3553.93, "Comentarios": "12 MTS DE ARENA y 12 MTS DE PIEDRÍN"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 842, "Código de proveedor": "PL-00106", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-7618B262-1707427544", "Fecha de vencimiento": "2025-12-17", "Importe": 3553.93, "Comentarios": "12 MTS DE ARENA Y 12 MTS DE PIEDRÍN"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 774, "Código de proveedor": "PL-00147", "Nombre de acreedor": "MARCO VINICIO LAZARO MANCIO", "No.Ref.del acreedor": "FC-5C3BBBEE-2476494058", "Fecha de vencimiento": "2025-09-17", "Importe": 6152.32, "Comentarios": "Anticipo del 20% de suministro y aplicación de texura en sanarate"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 729, "Código de proveedor": "PL-00148", "Nombre de acreedor": "PABLO CESAR BARRERA ROJAS", "No.Ref.del acreedor": "FC-REINTEGRO-0", "Fecha de vencimiento": "2025-08-06", "Importe": 60.0, "Comentarios": "combustible para uso de proyecto."}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 435, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-F56D1EBE-2177384766", "Fecha de vencimiento": "2024-11-13", "Importe": 18515.0, "Comentarios": "Servicios administrativos de la unidad del 1 de octubre al 31 de octubre"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 439, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-3BDD4AA6-1550861893", "Fecha de vencimiento": "2024-11-20", "Importe": 37030.0, "Comentarios": "Servicios administrativos de la unidad del 1 de noviembre al 30 de noviembre"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 479, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-8C3A2592-2483636380", "Fecha de vencimiento": "2025-01-08", "Importe": 37030.0, "Comentarios": "Servicios administrativos de la unidad del 1 al 31 de diciembre"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 495, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-A2FAA76D-2632600256", "Fecha de vencimiento": "2025-01-22", "Importe": 37030.0, "Comentarios": "Servicios administrativos de la unidad del 1 de enero al 31 enero"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 572, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-BE634047-1049578695", "Fecha de vencimiento": "2025-05-14", "Importe": 37030.0, "Comentarios": "Servicios administrativos de la unidad del 1 al 30 de abril"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 593, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-03A97DDB-2165065001", "Fecha de vencimiento": "2025-06-04", "Importe": 37030.0, "Comentarios": "Servicios administrativos de la unidad del 1 al 28 de febrero 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 596, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-298B71D7-992690696", "Fecha de vencimiento": "2025-06-04", "Importe": 37030.0, "Comentarios": "Servicios administrativos de la unidad del 1 al 31 de marzo 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 691, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-192DF4C0-1308639453", "Fecha de vencimiento": "2025-07-23", "Importe": 37030.0, "Comentarios": "Servicios Administrativos de la Unidad del 01 de junio al 30 de junio"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 701, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D3215511-393561042", "Fecha de vencimiento": "2025-08-13", "Importe": 21816.21, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 01 de junio al 30 de junio"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 707, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-CD5B875A-2809548045", "Fecha de vencimiento": "2025-08-20", "Importe": 37030.0, "Comentarios": "servicios administrativos del 01 al 31 de mayo 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 739, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-525870C2-1232093368", "Fecha de vencimiento": "2025-08-20", "Importe": 37030.0, "Comentarios": "Servicios de administración contable y financiera del 01 de Agosto al 31 de Agosto del 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 734, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-7C372D00-2827044648", "Fecha de vencimiento": "2025-09-10", "Importe": 37030.0, "Comentarios": "Servicios de administración contable y financiera del 01de julio al 31 de julio del 2025."}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 775, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-8D889F70-2026326731", "Fecha de vencimiento": "2025-09-17", "Importe": 15038.06, "Comentarios": "Servicios administrativos de la unidad mes de agosto"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 781, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-DB014BAF-4293152457", "Fecha de vencimiento": "2025-09-24", "Importe": 37030.0, "Comentarios": "Fee mes de septiembre 2025"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 794, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-BF164C2B-2140554491", "Fecha de vencimiento": "2025-10-08", "Importe": 15043.06, "Comentarios": "ENTREGA DE FORMULARIO BANRURAL SERVICIOS GENERALES CCC Y Re-Facturacion-Servicios Generales CCC Planilla Operativa"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 837, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-7EF5C67E-3053208962", "Fecha de vencimiento": "2025-12-10", "Importe": 15345.3, "Comentarios": "Ingreso de mandato al AGP, honorarios procurador Servicios Generales, Re-Facturacion-Servicios Generales CCC Planilla Operativa"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 846, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-B4058300-3882893401", "Fecha de vencimiento": "2025-12-10", "Importe": 15038.06, "Comentarios": "Re-Facturacion-Servicios Generales CCC Planilla Operativa"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 873, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-1CE8313C-110053608", "Fecha de vencimiento": "2026-01-21", "Importe": 14739.56, "Comentarios": "Re-Facturacion-Servicios Generales CCC Planilla Operativa, honorarios procurador 5%, Finca 353, folio 353, libro 1E de El Progreso, Desmembración y CV de la fracción desmembrada, comision procuracion"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 945, "Código de proveedor": "PL-00003", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-6BC702C1-2758036379", "Fecha de vencimiento": "2026-05-08", "Importe": 40932.99, "Comentarios": "honorarios procurador, Reingreso escritura CV y PLANILLA OPERATIVA"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 904, "Código de proveedor": "PL-00088", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-04FAB648-1964392796", "Fecha de vencimiento": "2026-03-11", "Importe": 11103.04, "Comentarios": "PROCESO DE ESCRITURACIÓN CONDADO LA CEIBA #4"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 950, "Código de proveedor": "PL-00152", "Nombre de acreedor": "RONI MARROQUIN HERNANDEZ", "No.Ref.del acreedor": "FC-4BCAE81A-1862551757", "Fecha de vencimiento": "2026-05-15", "Importe": 10487.81, "Comentarios": "1 ayudantes mes de mayo"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 815, "Código de proveedor": "PL-00087", "Nombre de acreedor": "SELVYN FERNANDO VELÁSQUEZ MARTINES", "No.Ref.del acreedor": "FC-02931796-2232503060", "Fecha de vencimiento": "2025-11-05", "Importe": 4952.06, "Comentarios": "PRIMERA ESTIMACION AGUA POTABLE 1RA. AVENIDA CONDADO LA CEIBA SANARATE"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 814, "Código de proveedor": "PL-00087", "Nombre de acreedor": "SELVYN FERNANDO VELÁSQUEZ MARTINES", "No.Ref.del acreedor": "FC-65955F78-2489337314", "Fecha de vencimiento": "2025-11-12", "Importe": 9934.77, "Comentarios": "TERCERA ESTIMACION HIDROSANITARIOS SEGUNDA AVENIDA, CONDADO LA CEIBA SANARATE"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 816, "Código de proveedor": "PL-00087", "Nombre de acreedor": "SELVYN FERNANDO VELÁSQUEZ MARTINES", "No.Ref.del acreedor": "FC-81A8F996-287000063", "Fecha de vencimiento": "2025-11-12", "Importe": 3289.25, "Comentarios": "PRIMERA ESTIMACION AGUA POTABLE 4TA. CALLE CONDADO LA CEIBA SANARATE"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 943, "Código de proveedor": "PL-00087", "Nombre de acreedor": "SELVYN FERNANDO VELÁSQUEZ MARTINES", "No.Ref.del acreedor": "FC-86D5559E-3792521407", "Fecha de vencimiento": "2026-05-13", "Importe": 14535.0, "Comentarios": "Por segunda estimación de pozo de absorción en segunda calle y segunda avenida condado la Ceiba Sanarate"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 949, "Código de proveedor": "PL-00160", "Nombre de acreedor": "SERGIO FRANCISCO CASTILLO OVALLE", "No.Ref.del acreedor": "FC-CAJA CHICA-0", "Fecha de vencimiento": "2026-05-15", "Importe": 1800.0, "Comentarios": "Honorarios a banrural por tercera liberacion condado la ceiba 15 unidades"}, {"EMPRESA": "SERVICIOS GENERALES CCC", "Nº documento": 948, "Código de proveedor": "PL-00096", "Nombre de acreedor": "WENDY DELFINA CASTILLO DE LEÓN", "No.Ref.del acreedor": "FC-REINTEGRO-0", "Fecha de vencimiento": "2026-05-15", "Importe": 800.0, "Comentarios": "Reparación de Chapeadora completa"}, {"EMPRESA": "CORCOLLE", "Nº documento": 1000045, "Código de proveedor": "PL-00032", "Nombre de acreedor": "CABISA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "D16D3470-2223523267", "Fecha de vencimiento": "2026-05-22", "Importe": 1800.0, "Comentarios": "D16D3470-8488-472B-A362-A10752C14F7F | Procesado por RV4 APAgent | TKT 00661"}, {"EMPRESA": "CORCOLLE", "Nº documento": 1000047, "Código de proveedor": "PL-00032", "Nombre de acreedor": "CABISA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "EFB45CA-3893119580", "Fecha de vencimiento": "2026-05-22", "Importe": 1800.0, "Comentarios": "EF8D45CA-E80C-4A5C-9D36-25CC446DB070 | Procesado por RV4 APAgent | TKT 00700"}, {"EMPRESA": "CORCOLLE", "Nº documento": 1000046, "Código de proveedor": "PL-00032", "Nombre de acreedor": "CABISA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "BACE3273-227231567", "Fecha de vencimiento": "2026-05-15", "Importe": 1800.0, "Comentarios": "BACE3273-0D8B-474F-93FD-51504F158C03 | Procesado por RV4 APAgent | TKT 00662"}, {"EMPRESA": "CORCOLLE", "Nº documento": 1000044, "Código de proveedor": "PL-00029", "Nombre de acreedor": "CLARA LÍLY CASTELLANOS RIZZO", "No.Ref.del acreedor": "FPQ-57915AE9-4112271760", "Fecha de vencimiento": "2026-05-15", "Importe": 10000.0, "Comentarios": "Comision de tierra los esclavos mes de abril 2026"}, {"EMPRESA": "CORCOLLE", "Nº documento": 1000043, "Código de proveedor": "PL-00030", "Nombre de acreedor": "WENDY DELFINA CASTILLO DE LEÓN", "No.Ref.del acreedor": "FC-REINTEGRO-0", "Fecha de vencimiento": "2026-05-15", "Importe": 900.0, "Comentarios": "Pago de flete para envio de toldo y mesas al proyecto de hacienda el cafetal los esclavos"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1000355, "Código de proveedor": "PL-00101", "Nombre de acreedor": "ARTE METAL Y LONA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-6AA1FA1F-1018839619", "Fecha de vencimiento": "2024-04-24", "Importe": 17400.0, "Comentarios": "2 Toldos desmontables 6x6 metros Basado en Solicitud de compra 296. Basado en Pedidos 286."}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001336, "Código de proveedor": "PL-00214", "Nombre de acreedor": "CABISA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "2D4A41AE-3375646484", "Fecha de vencimiento": "2026-05-27", "Importe": 2500.0, "Comentarios": "2D4A41AE-C934-4714-B9BD-83BD6A2C4D6B | Procesado por RV4 APAgent | TKT 00660"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001337, "Código de proveedor": "PL-00214", "Nombre de acreedor": "CABISA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "787BFBE3-457655473", "Fecha de vencimiento": "2026-05-27", "Importe": 2500.0, "Comentarios": "787BFBE3-1B47-44B1-AF26-654E33BFF798 | Procesado por RV4 APAgent | TKT 00665"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001328, "Código de proveedor": "PL-00212", "Nombre de acreedor": "CARLOS DAVID BARRIOS ESCOBAR COPROPIEDAD", "No.Ref.del acreedor": "FC-F1E66BF7-3515633381", "Fecha de vencimiento": "2026-04-10", "Importe": 65388.07, "Comentarios": "60% Anticipo fabricación e instalación Estructura Metálica según orden de Compra No. 1,128"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001036, "Código de proveedor": "PL-00020", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-16117C9E-1965181050", "Fecha de vencimiento": "2025-06-11", "Importe": 34499.96, "Comentarios": "Servicios administrativos de la unidad del 1 de junio  al 31 de junio"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001081, "Código de proveedor": "PL-00020", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-E101B8B4-1198409064", "Fecha de vencimiento": "2025-07-11", "Importe": 999.92, "Comentarios": "Servicios administrativos del mes del julio 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001115, "Código de proveedor": "PL-00020", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-BEABF90B-3606400731", "Fecha de vencimiento": "2025-08-13", "Importe": 68999.92, "Comentarios": "Gastos administrativos hacienda jumay mes de agosto 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001182, "Código de proveedor": "PL-00020", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-BDABE59C-4092938596", "Fecha de vencimiento": "2025-09-10", "Importe": 68999.92, "Comentarios": "Gastos administrativos hacienda jumay mes de septiembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001210, "Código de proveedor": "PL-00020", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-8604EA15-1187204196", "Fecha de vencimiento": "2025-10-15", "Importe": 68999.92, "Comentarios": "Gastos administrativos hacienda jumay mes de octubre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001238, "Código de proveedor": "PL-00020", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-1F79EA30-2010597180", "Fecha de vencimiento": "2025-11-19", "Importe": 68999.92, "Comentarios": "Gastos administrativos hacienda jumay mes de noviembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001296, "Código de proveedor": "PL-00012", "Nombre de acreedor": "CORPORACION FIRST CLASS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-393BFB86-3538832430", "Fecha de vencimiento": "2026-01-21", "Importe": 45850.0, "Comentarios": "630 CEMENTO 4060 PSI"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001300, "Código de proveedor": "PL-00012", "Nombre de acreedor": "CORPORACION FIRST CLASS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-774A36F1-4124921421", "Fecha de vencimiento": "2026-02-04", "Importe": 45850.0, "Comentarios": "630 Sacos de cemento + Flete"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001339, "Código de proveedor": "PL-00129", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-EEF0C869-105794515", "Fecha de vencimiento": "2026-05-15", "Importe": 266072.95, "Comentarios": "Servicios prestados del mes de abril 2026"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001340, "Código de proveedor": "PL-00196", "Nombre de acreedor": "EDGAR LEONEL PINTO PALOMO", "No.Ref.del acreedor": "FC-CAJA CHICA-0", "Fecha de vencimiento": "2026-05-15", "Importe": 400.0, "Comentarios": "GASOLINA PARA EQUIPOS"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001284, "Código de proveedor": "PL-00002", "Nombre de acreedor": "GRUPO CONSERSA, S.A.", "No.Ref.del acreedor": "FC-5CAC6EE9-3972612366", "Fecha de vencimiento": "2026-01-07", "Importe": 63680.0, "Comentarios": "Servicios administrativos del mes de septiembre"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001236, "Código de proveedor": "PL-00039", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-AD5D2F84-3597877786", "Fecha de vencimiento": "2025-11-26", "Importe": 10522.36, "Comentarios": "Pago 24/24 venta terreno Hacienda Jumay, Jalapa mes de noviembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001114, "Código de proveedor": "PL-00180", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-FB9E2623-3054780418", "Fecha de vencimiento": "2025-08-13", "Importe": 20816.56, "Comentarios": "Concreto para planta de tratamiento"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001172, "Código de proveedor": "PL-00180", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-677250CD-466898702", "Fecha de vencimiento": "2025-09-03", "Importe": 8851.85, "Comentarios": "Materiales para Planta de Tratamiento hacienda Jumay"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001290, "Código de proveedor": "PL-00180", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-DB58499A-497107720", "Fecha de vencimiento": "2026-01-07", "Importe": 21961.32, "Comentarios": "15m3 Concreto 4,003 convencional para cimentacion, 15m3 Colocacion de concreto con bomba Y 2 Laboratorios de concreto"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001335, "Código de proveedor": "PL-00180", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-615DECDB-206129289", "Fecha de vencimiento": "2026-05-15", "Importe": 21961.32, "Comentarios": "CONCRETO BOMBEO Y COLOCACION CON BOMBA"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001220, "Código de proveedor": "PL-00137", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-D8B25498-3187818549", "Fecha de vencimiento": "2025-10-22", "Importe": 23100.0, "Comentarios": "AGENTES DE SEGURIDAD 48X48 JUNIO"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001221, "Código de proveedor": "PL-00137", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-06272E6B-2985773186", "Fecha de vencimiento": "2025-10-22", "Importe": 23100.0, "Comentarios": "AGENTES DE SEGURIDAD 48X48 MAYO"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001222, "Código de proveedor": "PL-00137", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-82E2722B-3269935366", "Fecha de vencimiento": "2025-10-22", "Importe": 23100.0, "Comentarios": "AGENTES DE SEGURIDAD 48X48 ABRIL"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001107, "Código de proveedor": "PL-00172", "Nombre de acreedor": "MARVIN ULISES FUENTES FUENTES", "No.Ref.del acreedor": "FC-D6F32B99-162939752", "Fecha de vencimiento": "2025-08-06", "Importe": 4010.0, "Comentarios": "Medidor (contador) CL 200 electrónico 120-480v 4w FM 16S de 7 Terminales marca General Electric"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001100, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-914CA132-336610920", "Fecha de vencimiento": "2025-08-06", "Importe": 22516.22, "Comentarios": "Servicios administrativos de la unidad del mes de julio  2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001127, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-284A49E3-2884455255", "Fecha de vencimiento": "2025-08-20", "Importe": 167410.0, "Comentarios": "Servicios administrativos de la unidad del mes de agosto  2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001112, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-BBE92EF5F-1042369045", "Fecha de vencimiento": "2025-09-10", "Importe": 15209.02, "Comentarios": "Servicios de logistica, almacenamiento y distribucion de bienes del 1 al 31 de mayo 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001186, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-9AAAFF1B-4221059398", "Fecha de vencimiento": "2025-09-17", "Importe": 15038.06, "Comentarios": "Servcios administrativos de la unidad mes de agosto"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001190, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-8AAAA3D5-1904756267", "Fecha de vencimiento": "2025-09-17", "Importe": 167410.0, "Comentarios": "Servicios administrativos de la unidad mes de septiembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001204, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-96AC9089-1527333296", "Fecha de vencimiento": "2025-10-08", "Importe": 15038.06, "Comentarios": "Servicios administrativos de la unidad"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001223, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-C0D45BC4-2752728331", "Fecha de vencimiento": "2025-10-29", "Importe": 167410.0, "Comentarios": "Servicios contables y financiero del 01 al 31 de octubre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001255, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-A9C0EEDB-2598588240", "Fecha de vencimiento": "2025-12-03", "Importe": 167410.0, "Comentarios": "Servicios de administracion contable y financiera del 01 al 30 de noviembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001245, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-54ED8BD9-3010808200", "Fecha de vencimiento": "2025-12-10", "Importe": 15543.53, "Comentarios": "comision procuracion, Ingreso de crédito al RGP, Ingreso compraventa al RGP, Reingreso crédito al RGP, honorarios procurador Eficiencia Urbana,"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001265, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-CCAFEDF5-2088783340", "Fecha de vencimiento": "2025-12-10", "Importe": 16121.42, "Comentarios": "envio de documentos para aviso a la muni por compraventa de eficiencia urbana, OD Guatemala y Compania Limitada, DOCUMENTOS PARA ACTUALIZACION DE DATOS EFU EN BANRURAL, RECOLECCION FORMULARIOS DE FELTRE, ESSENZIALE Y EFU y Re-Facturacion-Servicios Genera"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001288, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-94A7A683-3679407692", "Fecha de vencimiento": "2026-01-07", "Importe": 167410.0, "Comentarios": "Servicios de administracion contable y financiera del 01 de diciembre al 31 de diciembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001295, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-9F26B0B6-1462649491", "Fecha de vencimiento": "2026-01-21", "Importe": 16294.65, "Comentarios": "Planilla Operativa, envio de documentacion para avisos, Gastos Credito, recepcion  de documentacion para avisos, Multa Aviso Dicabi, comision procuracion, Finca 6082, folio 82 libro 73E de Jalapa, Inscripción de registro Dicabi."}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001316, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-87C4ADF1-3767289837", "Fecha de vencimiento": "2026-03-18", "Importe": 41007.77, "Comentarios": "6 TALONARIOS DE RECIBOS DE CAJA TAMAÑO 1/2 CA Y  PLANILA OPERATIVA"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001193, "Código de proveedor": "PL-00136", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-04C42F75-2482654135", "Fecha de vencimiento": "2025-09-24", "Importe": 13667.47, "Comentarios": "HACIENDA JUMAY (JALAPA) ESCRITURACIÓN"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001194, "Código de proveedor": "PL-00136", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-2677337D-2450607251", "Fecha de vencimiento": "2025-09-24", "Importe": 49018.04, "Comentarios": "Servicios prestados en hacienda jumay estructuracion mes de septiembre 2025"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001231, "Código de proveedor": "PL-00136", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-618BCE84-2272021562", "Fecha de vencimiento": "2025-11-12", "Importe": 18214.56, "Comentarios": "HACIENDA JUMAY (JALAPA) ESCRITURACIÓN MES DE OCTUBRE@@SERVICIOS PRESTADOS [NT-171505]"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001338, "Código de proveedor": "PL-00210", "Nombre de acreedor": "SERGIO FRANCISCO CASTILLO OVALLE", "No.Ref.del acreedor": "FC-REINTEGRO-1152", "Fecha de vencimiento": "2026-05-15", "Importe": 4815.0, "Comentarios": "Pago de multas y timbres notariales para gestiones liberaciones de inmuebles en hacienda jumay Y Pago de envio de escrituras para firma"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1001227, "Código de proveedor": "PL-00146", "Nombre de acreedor": "SOTERO DÍAZ DE LA CRÚZ", "No.Ref.del acreedor": "FC-22BF95D0-4217850437", "Fecha de vencimiento": "2025-11-12", "Importe": 39190.0, "Comentarios": "Estimaciión de Pavimentación de la 8a, Avenida"}, {"EMPRESA": "EFICIENCIA URBANA", "Nº documento": 1000510, "Código de proveedor": "PL-00135", "Nombre de acreedor": "YENI CORINA LORENZANA ESTRADA", "No.Ref.del acreedor": "FC-AD37E464-1335250675", "Fecha de vencimiento": "2024-07-31", "Importe": 9615.0, "Comentarios": "1655 m2 Grama de area social"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000148, "Código de proveedor": "PL-00063", "Nombre de acreedor": "AURA CORP, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-EB24A50F-2344239897", "Fecha de vencimiento": "2025-07-02", "Importe": 12096.0, "Comentarios": "Por culminación fase 1 de diseño 40% y Anticipo fase 2 desarrollo de diseño 60%"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000116, "Código de proveedor": "PL-00009", "Nombre de acreedor": "Corporacion Consba, S.A.", "No.Ref.del acreedor": "FC-445C1D22-1526811401", "Fecha de vencimiento": "2025-04-02", "Importe": 5400.0, "Comentarios": "Plantilla de ayudantes segunda quincena de marzo"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000152, "Código de proveedor": "PL-00009", "Nombre de acreedor": "Corporacion Consba, S.A.", "No.Ref.del acreedor": "FC-97547314-573459450", "Fecha de vencimiento": "2025-07-02", "Importe": 16637.99, "Comentarios": "Por trabajos reestructura en garita"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000236, "Código de proveedor": "PL-00055", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-7B506B0A-4004269743", "Fecha de vencimiento": "2026-05-15", "Importe": 48894.08, "Comentarios": "Servicios prestados del mes de abril año 2026"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000215, "Código de proveedor": "PL-00092", "Nombre de acreedor": "GONZALEZ JUAREZ, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-D6338938-1845250761", "Fecha de vencimiento": "2025-12-17", "Importe": 7840.0, "Comentarios": "Servicios Profesionales de Auditoría Externa a sus Estados Financieros correspondientes del 01 de enero al 31 de diciembre 2025. Cuota 1/2 (50%) contra inicio de trabajo de campo.q"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000136, "Código de proveedor": "PL-00056", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-926F24BC-2829535416", "Fecha de vencimiento": "2025-06-04", "Importe": 8359.37, "Comentarios": "5 camionadas de selecto para condado Zacapa"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000161, "Código de proveedor": "PL-00056", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-12707F0A-3928441132", "Fecha de vencimiento": "2025-07-23", "Importe": 10031.25, "Comentarios": "6 Camionadas de Selecto"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000162, "Código de proveedor": "PL-00056", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-214B1E46-421021570", "Fecha de vencimiento": "2025-07-30", "Importe": 17425.71, "Comentarios": "48 mts de piedrín y 48 mts de arena"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000178, "Código de proveedor": "PL-00056", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-460D0A0C-3808444754", "Fecha de vencimiento": "2025-09-17", "Importe": 19250.45, "Comentarios": "36 M3 DE ARENA, 5 CAMIONADAS DE SELECTO y 24 M3 DE PIEDRÍN"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000074, "Código de proveedor": "PL-00025", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-446B92F3-4134489970", "Fecha de vencimiento": "2024-11-20", "Importe": 11100.0, "Comentarios": "2 Agentes de seguridad de 48x48 del 29 de junio al 28 de julio"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000075, "Código de proveedor": "PL-00025", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-8CBF01D7-541806342", "Fecha de vencimiento": "2024-12-04", "Importe": 11100.0, "Comentarios": "2 Agentes de seguridad de 48x48 del 29 de julio al 28 de agosto"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000118, "Código de proveedor": "PL-00025", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-9334458F-1126450407", "Fecha de vencimiento": "2025-04-09", "Importe": 11100.0, "Comentarios": "2 Agentes de seguridad de 48x48"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000001, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN, S.A.", "No.Ref.del acreedor": "FC-FF4B0B54-3896461589", "Fecha de vencimiento": "2023-11-30", "Importe": 11340.75, "Comentarios": "GASTOS ADMINISTRATIVOS MES DE NOVIEMBRE 2023"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000010, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN, S.A.", "No.Ref.del acreedor": "FE-B63F03D5-1631537063", "Fecha de vencimiento": "2023-12-28", "Importe": 89794.04, "Comentarios": NaN}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000228, "Código de proveedor": "PL-00036", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-79C767A8-161762292", "Fecha de vencimiento": "2026-03-11", "Importe": 5317.4, "Comentarios": "Servicios prestados mes de enero 2026 Oasis Zacapa"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000229, "Código de proveedor": "PL-00036", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-F49133B3-1831617848", "Fecha de vencimiento": "2026-03-11", "Importe": 5317.4, "Comentarios": "Servicios prestados mes de febrero 2026 Oasis Zacapa"}, {"EMPRESA": "FRUGALEX", "Nº documento": 1000107, "Código de proveedor": "PL-00059", "Nombre de acreedor": "WATERMANIA SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-63F6BCD3-2702920673", "Fecha de vencimiento": "2025-03-26", "Importe": 161425.0, "Comentarios": "Piscina condado zacapa"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000027, "Código de proveedor": "PL-00018", "Nombre de acreedor": "ARQUITECTURA SIETE, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-8DEC193C-723927719", "Fecha de vencimiento": "2026-02-11", "Importe": 83000.0, "Comentarios": "Urbanizacion club el progreso"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000023, "Código de proveedor": "PL-00019", "Nombre de acreedor": "DESTAKA, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-CB62E22A-1426147232", "Fecha de vencimiento": "2026-01-21", "Importe": 4995.77, "Comentarios": "Vallas: Tamaño 6x3 mts Lamina de aluzing 26 Costanera de 3x2 Fundidas de concreto Con tenzores Impresión en vinil adhesivo full color Altura de base 7 mts Proyecto: Club Residencial El Progreso"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000010, "Código de proveedor": "PL-00008", "Nombre de acreedor": "EDITH NOHEMI RODAS VIVAR", "No.Ref.del acreedor": "FPQ-A8B20BA4-3971041597", "Fecha de vencimiento": "2025-12-10", "Importe": 4750.0, "Comentarios": "PAGO DEL ANTICIPIO DEL 50% PARA LA GESTION ANTE CONRED DE LA NORMA PARA LA REDUCCION DE DESASTRES NRD2, PARA EL PROYECTO DENOMINADO CLUB RESIDENCIAL EL PROGRESO."}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000040, "Código de proveedor": "PL-00029", "Nombre de acreedor": "GEOESTUDIOS SOCIEDAD ANONIMA", "No.Ref.del acreedor": "408FEA00-2968078401", "Fecha de vencimiento": "2026-05-20", "Importe": 17196.43, "Comentarios": "408FEA00-B0E9-4841-911B-8D78562E2659 | Procesado por RV4 APAgent | TKT 00575"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000031, "Código de proveedor": "PL-00016", "Nombre de acreedor": "GRUPO HERGU, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-15BF5C75-2189575012", "Fecha de vencimiento": "2026-02-18", "Importe": 75000.0, "Comentarios": "Valor Correspondiente al pago de la estimación No. 3 del Proyecto Club Residencial Progreso"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000039, "Código de proveedor": "PL-00028", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-CC1C8DDB-4160442342", "Fecha de vencimiento": "2026-05-15", "Importe": 81322.08, "Comentarios": "120 m3 de concreto asentamiento 4000 psi"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000019, "Código de proveedor": "PL-00015", "Nombre de acreedor": "JOSÉ MARIANO SANDOVAL POLANCO", "No.Ref.del acreedor": "FC-1F99A195-3645981668", "Fecha de vencimiento": "2026-01-07", "Importe": 8000.0, "Comentarios": "Anticipo del 50% por elaboracion de plan de gestion ambiental"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000011, "Código de proveedor": "PL-00009", "Nombre de acreedor": "NESTOR PABLO ESCOBAR DÁVILA", "No.Ref.del acreedor": "FC-EDBD9F7A-3034860405", "Fecha de vencimiento": "2025-12-10", "Importe": 30520.0, "Comentarios": "Diseño hidrosanitario proyecto CLUB RESIDENCIAL PROGRESO"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000020, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-C89D77DC-3287304798", "Fecha de vencimiento": "2026-01-21", "Importe": 211.2, "Comentarios": "honorarios procurador 5% y FINCA 6605 FOLIO 105 LIBRO 54E DE JUTIAPA"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000038, "Código de proveedor": "PL-00007", "Nombre de acreedor": "SOLUCIONES INMOBILIARIAS DON GARCIA SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D84F63AC-2045266572", "Fecha de vencimiento": "2026-05-15", "Importe": 4776.79, "Comentarios": "Cancelación Estudio Hidrogeológico proyecto URBANIZACIÓN CLUB RESIDENCIAL EL PROGRESO, EL PROGRESO JUTIAPA"}, {"EMPRESA": "GARBATELLA", "Nº documento": 1000030, "Código de proveedor": "PL-00002", "Nombre de acreedor": "VICTOR JACINTO VALDEZ MEZA", "No.Ref.del acreedor": "FC-E70E6467-298208045", "Fecha de vencimiento": "2026-02-18", "Importe": 16691.27, "Comentarios": "por trabajos de resane, grisiado, pisos y pintura en garita Área interna, club Residencial El Progreso Jutiapa"}, {"EMPRESA": "GIBRALEON", "Nº documento": 1000004, "Código de proveedor": "PL-00005", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-BF7F3F10-3619439628", "Fecha de vencimiento": "2025-10-15", "Importe": 354.21, "Comentarios": "HABILITACION DE LIBROS APERTURA DE SOCIEDAD"}, {"EMPRESA": "GIBRALEON", "Nº documento": 1000011, "Código de proveedor": "PL-00005", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-5A5B952C-3494792670", "Fecha de vencimiento": "2026-02-11", "Importe": 959.0, "Comentarios": "aviso de emisión de acciones y  Compra de timbres notariales y fiscales por ampliación de capital"}, {"EMPRESA": "GIBRALEON", "Nº documento": 1000010, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-0AEEBDD5-2604682587", "Fecha de vencimiento": "2025-12-31", "Importe": 540.41, "Comentarios": "ingreso aumento Gibraleon,  honorarios procurador 5%, Multa aumento, 6 Talonarios recibos de caja tamaño 1/2 carta origina"}, {"EMPRESA": "GIBRALEON", "Nº documento": 1000014, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-3CA35190-4077865768", "Fecha de vencimiento": "2026-04-22", "Importe": 41.62, "Comentarios": "honorarios procurador y  Presentar Avisos emisión de acciones"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000004, "Código de proveedor": "PL-00005", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-F5BB240B-3851175862", "Fecha de vencimiento": "2025-10-15", "Importe": 333.95, "Comentarios": "HABILITACION DE LIBROS APERTURA DE SOCIEDAD"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000009, "Código de proveedor": "PL-00005", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-A4C8F2B3-3590671607", "Fecha de vencimiento": "2026-02-11", "Importe": 959.0, "Comentarios": "aviso de emisión de acciones, Compra de timbres notariales y fiscales por ampliación de capital"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000001, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-08B0F782-3753396058", "Fecha de vencimiento": "2025-07-23", "Importe": 36.53, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 de junio al 30 ed junio 2025"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000002, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-004E1B5B-1710771045", "Fecha de vencimiento": "2025-08-13", "Importe": 560.0, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 al 31 de mayo 2025"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000003, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-767F1AA3-28396610", "Fecha de vencimiento": "2025-10-08", "Importe": 1.25, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 de agosto al 31 de agosto 2025"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000005, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-CCB7679E-3373548062", "Fecha de vencimiento": "2025-10-15", "Importe": 219.36, "Comentarios": "asamblea por aumento de capital Leofreni, arancel por gastos de procuracion Leofreni, asamblea por aumento de capital Leofreni"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000007, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-E9FD3B35-3045082839", "Fecha de vencimiento": "2025-12-10", "Importe": 81.41, "Comentarios": "comision procuracion, Finca 8389, folio 389, libro 17E de Santa Rosa y aviso emision de acciones"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000008, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-4288FBF7-466698927", "Fecha de vencimiento": "2025-12-31", "Importe": 250.4, "Comentarios": "honorarios procurador 5%, ingreso aumento leofreni,  Multa aumento"}, {"EMPRESA": "LEOFRENI", "Nº documento": 1000012, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-E73AA4FF-3942927184", "Fecha de vencimiento": "2026-04-22", "Importe": 41.62, "Comentarios": "honorarios procurador y  Presentar Avisos emisión de acciones"}, {"EMPRESA": "OTTAVIA", "Nº documento": 1000088, "Código de proveedor": "PL-00029", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-38B0DBEB-4118104151", "Fecha de vencimiento": "2026-04-29", "Importe": 1260.0, "Comentarios": "Servicios prestados del mes de marzo 2026"}, {"EMPRESA": "OTTAVIA", "Nº documento": 1000090, "Código de proveedor": "PL-00029", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-2541B64D-792414128", "Fecha de vencimiento": "2026-05-15", "Importe": 1260.0, "Comentarios": "Servicios prestados mes de abril 2026"}, {"EMPRESA": "OTTAVIA", "Nº documento": 1000089, "Código de proveedor": "PL-00011", "Nombre de acreedor": "GRUPO CONSERSA, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-6AC2D252-1702970767", "Fecha de vencimiento": "2026-04-29", "Importe": 15125.0, "Comentarios": "Anticipo de comisiones Cañadas de Jalapa"}, {"EMPRESA": "OTTAVIA", "Nº documento": 1000077, "Código de proveedor": "PL-00026", "Nombre de acreedor": "ROSA MARÍA PARADA MARTÍNEZ", "No.Ref.del acreedor": "FC-9E1D55B5-3953607382", "Fecha de vencimiento": "2026-03-18", "Importe": 4776.79, "Comentarios": "Pago No. 3 de Q.30,000.00 correspondiente mes de diciembre de 2025, del Proyecto Cañadas de Jalapa"}, {"EMPRESA": "OTTAVIA", "Nº documento": 1000085, "Código de proveedor": "PL-00026", "Nombre de acreedor": "ROSA MARÍA PARADA MARTÍNEZ", "No.Ref.del acreedor": "FC-ADD6CF60-749093904", "Fecha de vencimiento": "2026-04-29", "Importe": 4776.79, "Comentarios": "Pago No. 6 de Q.30,000.00 correspondiente al mes de marzo 2026 del Proyecto de Cañadas de Jalapa"}, {"EMPRESA": "OVEST", "Nº documento": 1000065, "Código de proveedor": "PL-00012", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-E050320E-643910797", "Fecha de vencimiento": "2026-02-11", "Importe": 735.0, "Comentarios": "Compra de timbres notariales y fiscales por ampliación de capital"}, {"EMPRESA": "OVEST", "Nº documento": 1000029, "Código de proveedor": "PL-00002", "Nombre de acreedor": "IDEAS Y SOLUCIONES CREATIVAS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-80A6C329-316559356", "Fecha de vencimiento": "2025-10-01", "Importe": 20616.0, "Comentarios": "2 Vallas Publicitarias elaboradas con lamina de aluzing calibre 26, remachadas a estructura de tuvo cuadrado de 1\" chapa 20, con 2 manos de pintura anticorrosiva, marcos de costanera de 3\" Sta Lucia"}, {"EMPRESA": "OVEST", "Nº documento": 1000069, "Código de proveedor": "PL-00023", "Nombre de acreedor": "OSCAR AUGUSTO RIVAS VILLANUEVA", "No.Ref.del acreedor": "FC-E929942E-2840349397", "Fecha de vencimiento": "2026-04-29", "Importe": 1067.84, "Comentarios": "Honorarios profesionales Servicios ejercicio de depositario mes de abril 2026"}, {"EMPRESA": "OVEST", "Nº documento": 1000070, "Código de proveedor": "PL-00023", "Nombre de acreedor": "OSCAR AUGUSTO RIVAS VILLANUEVA", "No.Ref.del acreedor": "FC-E4F22721-2537636739", "Fecha de vencimiento": "2026-05-29", "Importe": 1067.84, "Comentarios": "Honorarios profesionales servicios de ejercicio de depositario mes de mayo 2026"}, {"EMPRESA": "OVEST", "Nº documento": 1000056, "Código de proveedor": "PL-00020", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-ECF69E69-3939978450", "Fecha de vencimiento": "2025-12-17", "Importe": 10206.56, "Comentarios": "Servicios prestados en hacienda santa lucia mes de diciembre 2025"}, {"EMPRESA": "OVEST", "Nº documento": 1000016, "Código de proveedor": "PL-00019", "Nombre de acreedor": "SSC INMOBILIARIO, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-C81DED4A-360270834", "Fecha de vencimiento": "2025-09-03", "Importe": 15731.6, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 01 junio al 30 de junio  del 2025"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000082, "Código de proveedor": "PL-00016", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-EAC3DD36-1616726102", "Fecha de vencimiento": "2026-03-18", "Importe": 36130.0, "Comentarios": "Servicios logisticos y de planificación proyecto hacienda el sol jutiapa"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000084, "Código de proveedor": "PL-00032", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-7ABB3859-47858234", "Fecha de vencimiento": "2026-03-13", "Importe": 43256.01, "Comentarios": "Servicios prestados mes de febrero 2026"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000085, "Código de proveedor": "PL-00032", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-181EDAA6-401754446", "Fecha de vencimiento": "2026-04-29", "Importe": 43256.01, "Comentarios": "Servicios prestados del mes de marzo 2026"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000089, "Código de proveedor": "PL-00032", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-D856E1E3-2451655347", "Fecha de vencimiento": "2026-05-15", "Importe": 43256.01, "Comentarios": "Servicios prestados mes de abril 2026"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000071, "Código de proveedor": "PL-00040", "Nombre de acreedor": "GONZALEZ JUAREZ, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-F1F5ED26-106187753", "Fecha de vencimiento": "2025-12-03", "Importe": 10080.0, "Comentarios": "Servicios Profesionales de Auditoría Externa a sus Estados Financieros correspondientes del 01 de enero al 31 de diciembre 2025. Cuota 1/2 (50%) contra inicio de trabajo de campo."}, {"EMPRESA": "ROSSIO", "Nº documento": 1000054, "Código de proveedor": "PL-00008", "Nombre de acreedor": "IDEAS Y SOLUCIONES CREATIVAS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-C478841A-2444116800", "Fecha de vencimiento": "2025-10-08", "Importe": 35616.0, "Comentarios": "2 Vallas Publicitarias elaboradas con lamina de aluzing callibre 26, tubo de 1\" chapa de 20, con 2 manos de pintura anticorrosiva, marcos de costanera de 3\" lleva 2 bases verticales y 2 en diagonal,."}, {"EMPRESA": "ROSSIO", "Nº documento": 1000088, "Código de proveedor": "PL-00044", "Nombre de acreedor": "JOSE OVIDIO DE LEON CONDE", "No.Ref.del acreedor": "8DCABD35-3161278439", "Fecha de vencimiento": "2026-05-15", "Importe": 4000.0, "Comentarios": "8DCABD35-BC6D-47E7-B76D-B29C4A43198E | Procesado por RV4 APAgent | TKT 00574"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000050, "Código de proveedor": "PL-00037", "Nombre de acreedor": "PABLO CESAR BARRERA ROJAS", "No.Ref.del acreedor": "FC-REINTEGRO-0", "Fecha de vencimiento": "2025-09-24", "Importe": 1800.0, "Comentarios": "Reintegro de caja chica pago de pipas de agua"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000062, "Código de proveedor": "PL-00021", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-66465BEC-2434289496", "Fecha de vencimiento": "2025-10-22", "Importe": 6805.12, "Comentarios": "Posesion lotes en hacienda el sol jutiapa mes de octubre 2025"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000067, "Código de proveedor": "PL-00021", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-3C5608DD-2415217277", "Fecha de vencimiento": "2025-11-26", "Importe": 3315.2, "Comentarios": "POSESIÓN JUTIAPA (LOTES) MES DE NOVIEMBRE"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000068, "Código de proveedor": "PL-00021", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-BE04F0C8-2178501229", "Fecha de vencimiento": "2025-11-26", "Importe": 2352.0, "Comentarios": "CONSTITUCIÓN DE ASOCIACIÓN CIVIL HACIENDA EL SOL"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000086, "Código de proveedor": "PL-00010", "Nombre de acreedor": "SSC INMOBILIARIO, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-CE69C1AE-4258874635", "Fecha de vencimiento": "2026-04-29", "Importe": 10000.0, "Comentarios": "Corte, carga y limpiezas de terreno SYW30404, JA7K03596"}, {"EMPRESA": "ROSSIO", "Nº documento": 1000059, "Código de proveedor": "PL-00014", "Nombre de acreedor": "VICTOR JACINTO VALDEZ MEZA", "No.Ref.del acreedor": "FC-5466C185-136924432", "Fecha de vencimiento": "2025-10-22", "Importe": 26924.4, "Comentarios": "Mano de Obra por pavimentación de calle 50% anticipo meta 4, hacienda el sol"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000034, "Código de proveedor": "PL-00021", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-A759B1E9-104876967", "Fecha de vencimiento": "2024-12-13", "Importe": 42323.4, "Comentarios": "Servicios de logistica y aaesoria en tramites municipales"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000037, "Código de proveedor": "PL-00021", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-220DF9C1-2717994600", "Fecha de vencimiento": "2025-01-08", "Importe": 141398.53, "Comentarios": "Mano de obra Garita Club Campestre Primera Estimacion"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000056, "Código de proveedor": "PL-00021", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-A903E415-464535717", "Fecha de vencimiento": "2025-02-05", "Importe": 96291.43, "Comentarios": "Mano de obra Garita Club Campestre estimacion NO. 2"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000069, "Código de proveedor": "PL-00021", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-1BC51E4A-1416186986", "Fecha de vencimiento": "2025-04-23", "Importe": 72948.68, "Comentarios": "Mano de obra Garita Club Campestre"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000087, "Código de proveedor": "PL-00021", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-BB9CED93-4410785", "Fecha de vencimiento": "2025-05-28", "Importe": 100685.57, "Comentarios": "Mano de obra Garita Club Campestre Estimacion No. 4"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000088, "Código de proveedor": "PL-00021", "Nombre de acreedor": "CORPORACION CONSBA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-71128829-3018999161", "Fecha de vencimiento": "2025-05-28", "Importe": 18272.0, "Comentarios": "MO pavimentacion Primera Avenida"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000219, "Código de proveedor": "PL-00018", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-BCD447DB-1537556660", "Fecha de vencimiento": "2026-04-29", "Importe": 26329.31, "Comentarios": "Servicios prestados del mes de marzo 2026"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000221, "Código de proveedor": "PL-00018", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-9AB4797E-1668304449", "Fecha de vencimiento": "2026-05-15", "Importe": 27349.34, "Comentarios": "Servicios prestados del mes de abril 2026"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000070, "Código de proveedor": "PL-00029", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-4598D338-3150925570", "Fecha de vencimiento": "2025-04-30", "Importe": 30000.0, "Comentarios": "Pago 4/12 venta terreno Campestre Jumay, Jalapa abril 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000082, "Código de proveedor": "PL-00029", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-46459773-2237022860", "Fecha de vencimiento": "2025-05-28", "Importe": 53100.0, "Comentarios": "Pago 5/12 venta terreno Campestre Jumay, Jalapa mayo 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000099, "Código de proveedor": "PL-00029", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D5AA5388-412961676", "Fecha de vencimiento": "2025-06-25", "Importe": 53100.0, "Comentarios": "Pago 6/12 venta terreno Campestre Jumay, Jalapa junio 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000110, "Código de proveedor": "PL-00029", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-7037FCC0-4161751814", "Fecha de vencimiento": "2025-07-30", "Importe": 53100.0, "Comentarios": "Pago 7/12 venta terreno Campestre Jumay, Jalapa julio 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000122, "Código de proveedor": "PL-00029", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-F348D2F1-2941733957", "Fecha de vencimiento": "2025-08-27", "Importe": 53100.0, "Comentarios": "Pago 8/12 venta terreno Campestre Jumay, Jalapa agosto 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000142, "Código de proveedor": "PL-00029", "Nombre de acreedor": "INVERSIONES IKIGAI DE GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-6B7B0AC0-2714520062", "Fecha de vencimiento": "2025-09-24", "Importe": 53100.0, "Comentarios": "Pago 9/12 venta terreno Campestre Jumay, Jalapa septiembre 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000094, "Código de proveedor": "PL-00012", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-11A99A62-1773752296", "Fecha de vencimiento": "2025-06-11", "Importe": 18271.21, "Comentarios": "51 m3 de selecto"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000108, "Código de proveedor": "PL-00012", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-A25CBEFF-1162561939", "Fecha de vencimiento": "2025-07-16", "Importe": 20321.87, "Comentarios": "127m3 de arena y 96m3 de Piedrin"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000138, "Código de proveedor": "PL-00012", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-1E7FB3EB-964971147", "Fecha de vencimiento": "2025-09-10", "Importe": 21782.14, "Comentarios": "84 M3 DE ARENA y 36 M3 DE PIEDRÍN"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000154, "Código de proveedor": "PL-00012", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-0A8DA5E0-945374625", "Fecha de vencimiento": "2025-10-15", "Importe": 12897.32, "Comentarios": "9 Días de trabajo"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000158, "Código de proveedor": "PL-00012", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-E07C1151-3405202529", "Fecha de vencimiento": "2025-10-29", "Importe": 4356.43, "Comentarios": "12 M3 DE ARENA y 12 M3 DE PIEDRIN"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000199, "Código de proveedor": "PL-00012", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-8B3C6C40-3705816318", "Fecha de vencimiento": "2026-01-14", "Importe": 2400.0, "Comentarios": "12 MTS DE ARENA"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000162, "Código de proveedor": "PL-00055", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-160ECE45-1482703450", "Fecha de vencimiento": "2025-10-29", "Importe": 23100.0, "Comentarios": "Agentes de seguridad de 48x48 junio"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000163, "Código de proveedor": "PL-00055", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-2D4C11DA-2591244865", "Fecha de vencimiento": "2025-10-29", "Importe": 23100.0, "Comentarios": "2 AGENTES DE SEGURIDAD 48X48 MAYO"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000164, "Código de proveedor": "PL-00055", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-C7F0EDC3-2400209146", "Fecha de vencimiento": "2025-10-29", "Importe": 23100.0, "Comentarios": "2 AGENTES DE SEGURIDAD 48X48 ABRIL"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000043, "Código de proveedor": "PL-00025", "Nombre de acreedor": "LUIS GERARDO ROLDÁN GALINDO", "No.Ref.del acreedor": "FC-B4224F77-798575065", "Fecha de vencimiento": "2025-01-08", "Importe": 91387.46, "Comentarios": "Extrencion de linea trifasica del proyecto"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000114, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-B571CBBC-3504752833", "Fecha de vencimiento": "2025-07-16", "Importe": 7083.72, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 de junio al 30 de junio 2025"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000156, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-8AEEB633-491670916", "Fecha de vencimiento": "2025-10-15", "Importe": 279.09, "Comentarios": "asamblea por aumento de capital Tezzoli, arancel por gastos de procuracion Tezzoli, asamblea por aumento de capital Tezzoli y aumentos de capital Tezzoli"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000171, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-F6F85841-3762766322", "Fecha de vencimiento": "2025-11-19", "Importe": 549.57, "Comentarios": "Aviso de emision de acciones, comision procuracion, Ingreso al RGP de aportación, Presentación aviso DICABI Y honorarios procurador Tezzoli"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000214, "Código de proveedor": "PL-00011", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-7D417738-3250014624", "Fecha de vencimiento": "2026-03-25", "Importe": 59.82, "Comentarios": "honorarios procurador, Ingreso de servidumbre"}, {"EMPRESA": "TEZZOLI", "Nº documento": 1000212, "Código de proveedor": "PL-00017", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-73B6BE18-4072818653", "Fecha de vencimiento": "2026-03-11", "Importe": 17011.68, "Comentarios": "Servicios prestados mes de febrero 2026"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000004, "Código de proveedor": "PL-00005", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-F5BB240B-3851175862", "Fecha de vencimiento": "2025-10-15", "Importe": 333.95, "Comentarios": "HABILITACION DE LIBROS APERTURA DE SOCIEDAD"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000009, "Código de proveedor": "PL-00005", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-A4C8F2B3-3590671607", "Fecha de vencimiento": "2026-02-11", "Importe": 959.0, "Comentarios": "aviso de emisión de acciones, Compra de timbres notariales y fiscales por ampliación de capital"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000001, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-08B0F782-3753396058", "Fecha de vencimiento": "2025-07-23", "Importe": 36.53, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 de junio al 30 ed junio 2025"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000002, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-004E1B5B-1710771045", "Fecha de vencimiento": "2025-08-13", "Importe": 560.0, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 al 31 de mayo 2025"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000003, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-767F1AA3-28396610", "Fecha de vencimiento": "2025-10-08", "Importe": 1.25, "Comentarios": "Servicios de logistica almacenamiento y distribucion de bienes del 1 de agosto al 31 de agosto 2025"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000005, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-CCB7679E-3373548062", "Fecha de vencimiento": "2025-10-15", "Importe": 219.36, "Comentarios": "asamblea por aumento de capital Leofreni, arancel por gastos de procuracion Leofreni, asamblea por aumento de capital Leofreni"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000007, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-E9FD3B35-3045082839", "Fecha de vencimiento": "2025-12-10", "Importe": 81.41, "Comentarios": "comision procuracion, Finca 8389, folio 389, libro 17E de Santa Rosa y aviso emision de acciones"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000008, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-4288FBF7-466698927", "Fecha de vencimiento": "2025-12-31", "Importe": 250.4, "Comentarios": "honorarios procurador 5%, ingreso aumento leofreni,  Multa aumento"}, {"EMPRESA": "TALOCCI", "Nº documento": 1000012, "Código de proveedor": "PL-00001", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-E73AA4FF-3942927184", "Fecha de vencimiento": "2026-04-22", "Importe": 41.62, "Comentarios": "honorarios procurador y  Presentar Avisos emisión de acciones"}, {"EMPRESA": "URBIVA", "Nº documento": 1000123, "Código de proveedor": "PL00006", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-EB788E65-3157280630", "Fecha de vencimiento": "2026-04-29", "Importe": 1645.0, "Comentarios": "Servicios prestados del mes de marzo 2026"}, {"EMPRESA": "URBIVA", "Nº documento": 1000125, "Código de proveedor": "PL00006", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-247D84B8-3403369110", "Fecha de vencimiento": "2026-05-15", "Importe": 1645.0, "Comentarios": "Servicios prestados correspondientes al mes de abril 2026"}, {"EMPRESA": "URBIVA", "Nº documento": 1000124, "Código de proveedor": "PL00052", "Nombre de acreedor": "JOSE OVIDIO DE LEON CONDE", "No.Ref.del acreedor": "FC-D9F7B21D-1431587744", "Fecha de vencimiento": "2026-05-13", "Importe": 3000.0, "Comentarios": "D9F7B21D-5554-4BA0-B447-41F3EF67C6C4 | Procesado por RV4 APAgent | TKT 00573"}, {"EMPRESA": "UTILICA", "Nº documento": 1000379, "Código de proveedor": "PL-00115", "Nombre de acreedor": "ALFREDO ORELLANA BATRES", "No.Ref.del acreedor": "FPQ-B705B95A-2823376309", "Fecha de vencimiento": "2025-07-09", "Importe": 9962.5, "Comentarios": "7 CAMIONADAS DE ARENA, 7 CAMIONADAS DE PIEDRÍN Y 7 CAMIONADAS DE SELECTO"}, {"EMPRESA": "UTILICA", "Nº documento": 1000511, "Código de proveedor": "PL-00115", "Nombre de acreedor": "ALFREDO ORELLANA BATRES", "No.Ref.del acreedor": "FPQ-658C56FA-1908097361", "Fecha de vencimiento": "2025-11-26", "Importe": 3600.0, "Comentarios": "1 CAMIONADA DE ARENA Y 1 CAMIONADA DE PIEDRIN"}, {"EMPRESA": "UTILICA", "Nº documento": 1000530, "Código de proveedor": "PL-00115", "Nombre de acreedor": "ALFREDO ORELLANA BATRES", "No.Ref.del acreedor": "FPQ-FB716607-716607", "Fecha de vencimiento": "2025-12-17", "Importe": 1500.0, "Comentarios": "1 CAMIONADA DE ARENA"}, {"EMPRESA": "UTILICA", "Nº documento": 1000543, "Código de proveedor": "PL-00115", "Nombre de acreedor": "ALFREDO ORELLANA BATRES", "No.Ref.del acreedor": "FPQ-DAFC93C2-3486928505", "Fecha de vencimiento": "2026-01-07", "Importe": 21600.0, "Comentarios": "6 CAMIONADAS DE ARENA y 6 CAMIONADAS DE PIEDRIN"}, {"EMPRESA": "UTILICA", "Nº documento": 1000546, "Código de proveedor": "PL-00115", "Nombre de acreedor": "ALFREDO ORELLANA BATRES", "No.Ref.del acreedor": "FPQ-8D4607E1-3882697668", "Fecha de vencimiento": "2026-02-11", "Importe": 4750.0, "Comentarios": "5 CAMIONADAS DE SELECTO"}, {"EMPRESA": "UTILICA", "Nº documento": 1000516, "Código de proveedor": "PL-00109", "Nombre de acreedor": "ANGEL ARNOLDO HERNÁNDEZ CÁN", "No.Ref.del acreedor": "FC-99CFBA7D-735792934", "Fecha de vencimiento": "2025-11-26", "Importe": 1222.6, "Comentarios": "3ra estiamción de instalación de bordillo de la 2da avenida"}, {"EMPRESA": "UTILICA", "Nº documento": 1000515, "Código de proveedor": "PL-00109", "Nombre de acreedor": "ANGEL ARNOLDO HERNÁNDEZ CÁN", "No.Ref.del acreedor": "FC-30288965-723403410", "Fecha de vencimiento": "2025-12-03", "Importe": 2137.5, "Comentarios": "1ra estimación de instalación de bordillo de la 1ra avenida"}, {"EMPRESA": "UTILICA", "Nº documento": 1000520, "Código de proveedor": "PL-00109", "Nombre de acreedor": "ANGEL ARNOLDO HERNÁNDEZ CÁN", "No.Ref.del acreedor": "FC-96A36FF9-1987857133", "Fecha de vencimiento": "2025-12-10", "Importe": 15907.5, "Comentarios": "Estimación de pavimentación de encaminamiento de laguneta"}, {"EMPRESA": "UTILICA", "Nº documento": 1000521, "Código de proveedor": "PL-00109", "Nombre de acreedor": "ANGEL ARNOLDO HERNÁNDEZ CÁN", "No.Ref.del acreedor": "FC-D65339E5-3054126702", "Fecha de vencimiento": "2025-12-10", "Importe": 2722.77, "Comentarios": "Estimación No. 1 de instalación postes de acometida de casa modelo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000522, "Código de proveedor": "PL-00109", "Nombre de acreedor": "ANGEL ARNOLDO HERNÁNDEZ CÁN", "No.Ref.del acreedor": "FC-B0FB8D3C-2812497690", "Fecha de vencimiento": "2025-12-10", "Importe": 1622.25, "Comentarios": "Estimación No. 5 de hidrosanitarios."}, {"EMPRESA": "UTILICA", "Nº documento": 1000391, "Código de proveedor": "PL-00103", "Nombre de acreedor": "AXALTA GUATEMALA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-488923DE-1726499298", "Fecha de vencimiento": "2025-07-30", "Importe": 7040.0, "Comentarios": "128m2 acril techo power (Incluye material y aplicacion)"}, {"EMPRESA": "UTILICA", "Nº documento": 1000466, "Código de proveedor": "PL-00042", "Nombre de acreedor": "Corporacion Consba, S.A.", "No.Ref.del acreedor": "FC-854BDC58-1890861367", "Fecha de vencimiento": "2025-09-10", "Importe": 31385.5, "Comentarios": "Gastos administrativos 04 2024"}, {"EMPRESA": "UTILICA", "Nº documento": 1000506, "Código de proveedor": "PL-00042", "Nombre de acreedor": "Corporacion Consba, S.A.", "No.Ref.del acreedor": "FC-4F817BBA-1131433339", "Fecha de vencimiento": "2025-12-03", "Importe": 31385.0, "Comentarios": "Gastos administrativos 05- 2024"}, {"EMPRESA": "UTILICA", "Nº documento": 1000424, "Código de proveedor": "PL-00139", "Nombre de acreedor": "DESARROLLOS ARQUBO, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-50E89355-51134779", "Fecha de vencimiento": "2025-08-20", "Importe": 23525.0, "Comentarios": "Accesorios de cocina para casa modelo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000570, "Código de proveedor": "PL-00081", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-431BE4DD-2778875517", "Fecha de vencimiento": "2026-04-29", "Importe": 24379.7, "Comentarios": "Servicios prestados durante el mes de marzo 2026"}, {"EMPRESA": "UTILICA", "Nº documento": 100000007, "Código de proveedor": "PL-00081", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-CA1BB6CB-184699507", "Fecha de vencimiento": "2026-05-15", "Importe": 24379.7, "Comentarios": "Servicios prestados del mes de abril año 2026"}, {"EMPRESA": "UTILICA", "Nº documento": 1000523, "Código de proveedor": "PL-00099", "Nombre de acreedor": "EDWIN MIGUEL JIMÉNEZ PÉREZ", "No.Ref.del acreedor": "FC-D61D651E-2680375695", "Fecha de vencimiento": "2025-12-17", "Importe": 52803.0, "Comentarios": "Pago de instalaciones eléctricas estimación 1 de condado jutiapa"}, {"EMPRESA": "UTILICA", "Nº documento": 1000076, "Código de proveedor": "PL-00001", "Nombre de acreedor": "GRUPO CONSERSA, S.A.", "No.Ref.del acreedor": "FC-5E4D92BC-1480609452", "Fecha de vencimiento": "2024-04-01", "Importe": 41270.0, "Comentarios": "Fee es de Febrero 2024 Basado en Solicitud de compra 47. Basado en Pedidos 47."}, {"EMPRESA": "UTILICA", "Nº documento": 1000437, "Código de proveedor": "PL-00121", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-CEE22275-1085425195", "Fecha de vencimiento": "2025-09-10", "Importe": 14400.0, "Comentarios": "Tapaderas y rejillas hidrosanitarios 1ra avenida tramo 1"}, {"EMPRESA": "UTILICA", "Nº documento": 1000438, "Código de proveedor": "PL-00121", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-596C2EFA-362563504", "Fecha de vencimiento": "2025-09-10", "Importe": 4600.0, "Comentarios": "Tapaderas y rejillas hidrosanitarios 1ra calle (aguas negras y pluviales)"}, {"EMPRESA": "UTILICA", "Nº documento": 1000439, "Código de proveedor": "PL-00121", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D9A365C4-758139433", "Fecha de vencimiento": "2025-09-10", "Importe": 6050.0, "Comentarios": "Tapaderas y rejillas hidrosanitarios 1ra avenida A (Aguas negras y pluviales)"}, {"EMPRESA": "UTILICA", "Nº documento": 1000440, "Código de proveedor": "PL-00121", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D42E3A13-4179641861", "Fecha de vencimiento": "2025-09-10", "Importe": 3400.0, "Comentarios": "cajas prefabricadas para hidrosanitarios 2da calle condado jutiapa"}, {"EMPRESA": "UTILICA", "Nº documento": 1000541, "Código de proveedor": "PL-00121", "Nombre de acreedor": "GRUPO MOBIUS, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-51387077-625427082", "Fecha de vencimiento": "2026-01-07", "Importe": 4050.0, "Comentarios": "3 Tapaderas prefabricadas y Flete"}, {"EMPRESA": "UTILICA", "Nº documento": 1000187, "Código de proveedor": "PL-00077", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-28250787-888882294", "Fecha de vencimiento": "2024-12-11", "Importe": 68440.4, "Comentarios": "47m2 concreto"}, {"EMPRESA": "UTILICA", "Nº documento": 1000188, "Código de proveedor": "PL-00077", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-9D57F46D-4121576215", "Fecha de vencimiento": "2024-12-11", "Importe": 17213.28, "Comentarios": "14m2 concreto"}, {"EMPRESA": "UTILICA", "Nº documento": 1000208, "Código de proveedor": "PL-00077", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-31A36D1A-3172286997", "Fecha de vencimiento": "2025-02-05", "Importe": 21656.04, "Comentarios": "Concreto casa modelo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000225, "Código de proveedor": "PL-00077", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D8BB6B9C-1244086747", "Fecha de vencimiento": "2025-04-02", "Importe": 4751.04, "Comentarios": "4m3 de concreto incluye bombeo y colocacion con bomba, dosis de retardante, grupo de colocacion y bombeo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000298, "Código de proveedor": "PL-00077", "Nombre de acreedor": "INVERSIONES SALISBURY, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-67CF9A8D-1937589022", "Fecha de vencimiento": "2025-04-23", "Importe": 24515.4, "Comentarios": "Concreto casa modelo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000487, "Código de proveedor": "PL-00107", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-C080865B-1413038594", "Fecha de vencimiento": "2025-10-29", "Importe": 22856.92, "Comentarios": "14.5 Dias de Camión"}, {"EMPRESA": "UTILICA", "Nº documento": 1000507, "Código de proveedor": "PL-00107", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-0CC181CB-276710354", "Fecha de vencimiento": "2025-11-19", "Importe": 825.0, "Comentarios": "0.5 Día de camión"}, {"EMPRESA": "UTILICA", "Nº documento": 1000510, "Código de proveedor": "PL-00107", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-4941A7D5-944916168", "Fecha de vencimiento": "2025-11-19", "Importe": 15763.39, "Comentarios": "10 Días de Camión"}, {"EMPRESA": "UTILICA", "Nº documento": 1000532, "Código de proveedor": "PL-00107", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-AEFD1BC7-2006730919", "Fecha de vencimiento": "2025-12-17", "Importe": 15763.39, "Comentarios": "10 días de camión"}, {"EMPRESA": "UTILICA", "Nº documento": 1000533, "Código de proveedor": "PL-00107", "Nombre de acreedor": "JOSÉ ABELARDO ESTRADA CISNEROS", "No.Ref.del acreedor": "FC-F2E93DCA-777339494", "Fecha de vencimiento": "2025-12-17", "Importe": 15763.39, "Comentarios": "10 días de camión"}, {"EMPRESA": "UTILICA", "Nº documento": 1000491, "Código de proveedor": "PL-00156", "Nombre de acreedor": "JOSÉ ARNOLDO PADILLA SALAZAR", "No.Ref.del acreedor": "FC-D2420АЗС-4101849745", "Fecha de vencimiento": "2025-10-22", "Importe": 98000.0, "Comentarios": "500 Pies Perforazión de pozo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000477, "Código de proveedor": "PL-00104", "Nombre de acreedor": "JUAN PABLO CHIAPAS PÉREZ", "No.Ref.del acreedor": "FC-DE329C58-1433028541", "Fecha de vencimiento": "2025-09-24", "Importe": 5015.62, "Comentarios": "Servicios profesionales de 350 firmas y sellos de planos en condado jutiapa"}, {"EMPRESA": "UTILICA", "Nº documento": 100000002, "Código de proveedor": "PL-00079", "Nombre de acreedor": "KAYROS SECURITY, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-7CAEEA4E-190270243", "Fecha de vencimiento": "2026-01-14", "Importe": 11100.0, "Comentarios": "2.0 AGENTES DE SEGURIDAD 24X24 NOVIEMBRE 2025 Marzo 2025"}, {"EMPRESA": "UTILICA", "Nº documento": 1000497, "Código de proveedor": "PL-00155", "Nombre de acreedor": "LAS 4 A, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-1749A142-2351517676", "Fecha de vencimiento": "2025-11-05", "Importe": 7400.0, "Comentarios": "Renta de rodo del 25/09/2025 al 22/10/2025"}, {"EMPRESA": "UTILICA", "Nº documento": 1000345, "Código de proveedor": "PL-00136", "Nombre de acreedor": "LUIS GERARDO ROLDÁN GALINDO", "No.Ref.del acreedor": "FC-1D2FA07A-2640858738", "Fecha de vencimiento": "2025-05-21", "Importe": 45125.0, "Comentarios": "Mano de obra de electrificacion 13.2 kv en condado jutiapa 50%"}, {"EMPRESA": "UTILICA", "Nº documento": 1000387, "Código de proveedor": "PL-00097", "Nombre de acreedor": "MARBIN ALEXANDER GRIJALVA ARGUETA", "No.Ref.del acreedor": "FC-DA9D36CD-471879333", "Fecha de vencimiento": "2025-07-09", "Importe": 1952.51, "Comentarios": "Estimación 15 sobre obra gris de casa modelo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000336, "Código de proveedor": "PL-00056", "Nombre de acreedor": "MARIA ELENA RAQUEC CUJCUJ", "No.Ref.del acreedor": "FC-BBDA8912-1686981863", "Fecha de vencimiento": "2025-05-14", "Importe": 4000.0, "Comentarios": "Marcos de rejilla de la 1ra calle y 2da avendia"}, {"EMPRESA": "UTILICA", "Nº documento": 1000358, "Código de proveedor": "PL-00056", "Nombre de acreedor": "MARIA ELENA RAQUEC CUJCUJ", "No.Ref.del acreedor": "FC-FD3C8276-2446936492", "Fecha de vencimiento": "2025-06-04", "Importe": 4000.0, "Comentarios": "Barandas para 2da casa modelo en condado jutiapa"}, {"EMPRESA": "UTILICA", "Nº documento": 1000566, "Código de proveedor": "PL-00147", "Nombre de acreedor": "OMALI, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-8DDDA59A-1529432690", "Fecha de vencimiento": "2026-04-22", "Importe": 43179.72, "Comentarios": "INSTALACION DE GEOMEMBRANA PARA RECUBRIMIENTO DE RESERVORIO"}, {"EMPRESA": "UTILICA", "Nº documento": 1000065, "Código de proveedor": "PL-00005", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-E74522B4-4071113954", "Fecha de vencimiento": "2024-03-06", "Importe": 42090.0, "Comentarios": NaN}, {"EMPRESA": "UTILICA", "Nº documento": 1000073, "Código de proveedor": "PL-00005", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-50DACF4A-3849865033", "Fecha de vencimiento": "2024-04-03", "Importe": 42090.0, "Comentarios": "Basado en Solicitud de compra 61. Basado en Pedidos 60."}, {"EMPRESA": "UTILICA", "Nº documento": 1000086, "Código de proveedor": "PL-00005", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-521BD347-161697436", "Fecha de vencimiento": "2024-04-24", "Importe": 42090.0, "Comentarios": "Servicios administrativos de la unidad del 01 de abril al 30 de abril del 2024"}, {"EMPRESA": "UTILICA", "Nº documento": 1000561, "Código de proveedor": "PL-00005", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-F4ECFDF7-2851883787", "Fecha de vencimiento": "2026-03-25", "Importe": 2408.0, "Comentarios": "Pago de2da contancia de marca Utilica"}, {"EMPRESA": "UTILICA", "Nº documento": 1000321, "Código de proveedor": "PL-00119", "Nombre de acreedor": "PREFABRICADOS CIFA, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-C3C956BF-3736029099", "Fecha de vencimiento": "2025-05-14", "Importe": 75248.09, "Comentarios": "Postes de concreto para el proyecto"}, {"EMPRESA": "UTILICA", "Nº documento": 1000490, "Código de proveedor": "PL-00084", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-9962386B-3914876299", "Fecha de vencimiento": "2025-10-22", "Importe": 7214.0, "Comentarios": "Servicios prestados en condado jutiapa mes de octubre 2025"}, {"EMPRESA": "UTILICA", "Nº documento": 1000518, "Código de proveedor": "PL-00084", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-51F570C1-3314893882", "Fecha de vencimiento": "2025-11-26", "Importe": 30486.0, "Comentarios": "CONDADO JUTIAPA MES DE NOVIEMBRE"}, {"EMPRESA": "UTILICA", "Nº documento": 1000554, "Código de proveedor": "PL-00084", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-EBE70D7C-423970444", "Fecha de vencimiento": "2026-03-11", "Importe": 6272.0, "Comentarios": "Servicios prestados area comercial mes de enero 2026 condado jutiapa"}, {"EMPRESA": "UTILICA", "Nº documento": 1000555, "Código de proveedor": "PL-00084", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-B0D72573-814697987", "Fecha de vencimiento": "2026-03-11", "Importe": 6272.0, "Comentarios": "Servicios prestados area comercial condado jutiapa mes de febrero 2026"}, {"EMPRESA": "UTILICA", "Nº documento": 1000556, "Código de proveedor": "PL-00084", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-ABB57A91-3621538937", "Fecha de vencimiento": "2026-03-11", "Importe": 4030.39, "Comentarios": "Servicios prestados condado jutiapa mes de enero 2026"}, {"EMPRESA": "UTILICA", "Nº documento": 1000557, "Código de proveedor": "PL-00084", "Nombre de acreedor": "RODRIGUEZ, AGUILAR, CASTELLANOS, SOLARES Y ALVARADO,SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-5D3DA48E-2338079844", "Fecha de vencimiento": "2026-03-11", "Importe": 4030.39, "Comentarios": "Servicios prestados condado jutiapa mes de febrero 2026"}, {"EMPRESA": "UTILICA", "Nº documento": 1000341, "Código de proveedor": "PL-00123", "Nombre de acreedor": "SERVELIO BARRERA GONZÁLEZ", "No.Ref.del acreedor": "FC-E6E20078-1092110253", "Fecha de vencimiento": "2025-05-21", "Importe": 21382.04, "Comentarios": "Mano de obra instalacion de piso, azulejo y fachada casa modelo"}, {"EMPRESA": "UTILICA", "Nº documento": 1000373, "Código de proveedor": "PL-00110", "Nombre de acreedor": "TABLATEX, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-D388FCC7-2914471688", "Fecha de vencimiento": "2025-07-09", "Importe": 43601.15, "Comentarios": "Cancelación del 100% de suministro y aplicación de textura en casa 1 y 2 en obra Condado Jutiapa"}, {"EMPRESA": "UTILICA", "Nº documento": 1000426, "Código de proveedor": "PL-00110", "Nombre de acreedor": "TABLATEX, SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-0576AE92-2543603209", "Fecha de vencimiento": "2025-09-17", "Importe": 6228.74, "Comentarios": "Cancelación del 10% de suministros y aplicación de textura en casa modelo en obra Condado Jutiapa"}, {"EMPRESA": "VILET", "Nº documento": 1000006, "Código de proveedor": "PL-00002", "Nombre de acreedor": "DESARROLLOS STEL, SOCIEDAD ANÓNIMA", "No.Ref.del acreedor": "FC-8A40A582-2818719979", "Fecha de vencimiento": "2026-02-11", "Importe": 949.23, "Comentarios": "Gastos legales por apertura de nueva sociedad, aviso de emisión de acciones Vilet. Y Certificacion F6584 F158 L684 Vilet, arancel por gastos de procuracion vilet"}, {"EMPRESA": "VILET", "Nº documento": 1000003, "Código de proveedor": "PL-00003", "Nombre de acreedor": "GONZALEZ JUAREZ, SOCIEDAD CIVIL", "No.Ref.del acreedor": "FC-14016CBC-877480732", "Fecha de vencimiento": "2025-12-03", "Importe": 6720.0, "Comentarios": "Servicios Profesionales de Auditoría Externa a sus Estados Financieros correspondientes del 01 de enero al 31 de diciembre 2025. Cuota 1/2 (50%) contra inicio de trabajo de campo."}, {"EMPRESA": "VILET", "Nº documento": 1000007, "Código de proveedor": "PL-00005", "Nombre de acreedor": "PHIEN SOCIEDAD ANONIMA", "No.Ref.del acreedor": "FC-7B3A7458-3318695334", "Fecha de vencimiento": "2026-04-22", "Importe": 910.0, "Comentarios": "pastas nuevas sociedade"}];

function renderCXP() {
  filtrarCXP(); // initial render
}

window.EMPRESA_DISPLAY_MAP = {"EFICIENCIA URBANA": "Eficiencia Urbana — Hacienda Jumay", "SERVICIOS GENERALES CCC": "Servicios Generales — La Ceiba", "ROSSIO": "Rossio — Hacienda el Sol", "FRUGALEX": "Frugalex — Oasis Zacapa", "OTTAVIA": "Ottavia — Cañadas de Jalapa", "UTILICA": "Utilica — Condado Jutiapa", "TEZZOLI": "Tezzoli — Club Campestre Jumay", "URBIVA": "Urbiva — Club del Bosque", "GARBATELLA": "Garbatella — Club Residencial El Progreso", "CAPIPOS": "Capipos — Arboleda Santa Elena", "OVEST": "Ovest — Hacienda Santa Lucia", "CORCOLLE": "Corcolle — Hacienda El Cafetal Fase I", "LEOFRENI": "Leofreni — Hacienda El Cafetal Fase II", "GIBRALEON": "Gibraleón — Hacienda El Cafetal Fase III", "TALOCCI": "Talocci — Hacienda El Cafetal Fase IV", "VILET": "Vilet — Celajes De Tecpan"};
window.filtrarCXP = function() {
  const tbody = document.getElementById('cxpTbody');
  if (!tbody) return;
  const filtro = document.getElementById('cxpFiltroEmpresa')?.value || '';
  const hoy = new Date();
  const items = filtro ? CXP_DATA.filter(r => r['EMPRESA'] === filtro) : CXP_DATA;
  tbody.innerHTML = items.map(r => {
    const venc = r['Fecha de vencimiento'] ? new Date(r['Fecha de vencimiento']) : null;
    const vencida = venc && venc < hoy;
    const fechaTxt = r['Fecha de vencimiento'] || '—';
    return `<tr>
      <td style="padding:5px 10px;font-weight:600;font-size:11px">${r['EMPRESA']||'—'}</td>
      <td style="padding:5px 10px;font-size:11px">${(r['Nombre de acreedor']||'—').substring(0,40)}</td>
      <td style="padding:5px 10px;text-align:right;font-size:11px;color:${vencida?'#e05050':'inherit'};font-weight:${vencida?'700':'400'}">${fechaTxt}${vencida?' ⚠':''}</td>
      <td style="padding:5px 10px;text-align:right;font-weight:700;font-size:11px">Q ${Number(r['Importe']||0).toLocaleString('es-GT',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="4" style="text-align:center;padding:20px;color:var(--muted)">Sin documentos para esta sociedad</td></tr>';
  const totalEl = document.getElementById('cxpTotal');
  const total = items.reduce((s,r) => s+(Number(r['Importe'])||0), 0);
  if (totalEl) totalEl.textContent = `Total: Q ${total.toLocaleString('es-GT',{minimumFractionDigits:2})} · ${items.length} documentos`;
};


/* ── Índice de Proyectos (slide 32) ────────────────────── */
async function renderIndiceProyectos() {
  const tbody = document.getElementById('indiceTbody');
  if (!tbody) return;
  try {
    const inv = await apiFetch('/api/inventario/resumen');
    if (!inv || !inv.length) { tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:30px;color:var(--muted)">Sin datos de inventario</td></tr>'; return; }
    let i = 1;
    tbody.innerHTML = inv.map(r => `<tr>
      <td style="padding:6px 8px;text-align:center;font-weight:700;color:var(--muted)">${i++}</td>
      <td style="padding:6px 8px;font-weight:600">${r.proyecto||r.empresa||'—'}</td>
      <td style="padding:6px 8px;text-align:right">${r.total_lotes||'—'}</td>
      <td style="padding:6px 8px;text-align:right">${r.disponibles||'—'}</td>
      <td style="padding:6px 8px;text-align:right;font-weight:700;color:var(--dorado)">${r.pct_vendido ? (r.pct_vendido*100).toFixed(1)+'%' : '—'}</td>
      <td style="padding:6px 8px;text-align:right">—</td>
      <td style="padding:6px 8px;text-align:right">—</td>
    </tr>`).join('');
  } catch(e) {
    tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--red)">Error: ${e.message}</td></tr>`;
  }
}


/* ── Apply slides configuration from jd.html ──────────── */
function applySlideConfig() {
  const saved = localStorage.getItem('rv4_slides_config');
  if (!saved) return;
  const config = JSON.parse(saved);
  // Map config ids to slide data-screen-labels
  const idToLabel = {
    'portada':     ['01 Portada'],
    'agenda':      ['02 Agenda'],
    'minuta':      ['Minutas JD'],
    'resumen':     ['03 Resumen Ejecutivo'],
    'div-inv':     ['04 Divider Inventario'],
    'inv-consol':  ['05 Inventario Consolidado'],
    'absorcion':   ['06 Absorcion por Proyecto'],
    'ventas-anio': ['06b Ventas por Año'],
    'valor':       ['07 Valor por Proyecto'],
    'div-ventas':  ['08 Divider Ventas'],
    'ventas-res':  ['09 Ventas Resumen'],
    'tendencia':   ['10 Tendencia Ventas'],
    'mezcla':      ['11 Mezcla Financiera'],
    'vendedores':  ['12 Top Vendedores'],
    'metas':       ['13 Metas vs Avance'],
    'div-cartera': ['14 Divider Cartera'],
    'cartera-res': ['15 Cartera Resumen'],
    'aging':       ['16 Aging Cartera'],
    'morosos':     ['16b Morosos 61+'],
    'cobros':      ['17 Proyeccion Cobros'],
    'desist':      ['18 Desistimientos'],
    'alertas':     ['19 Alertas Cartera'],
    'div-flujos':  ['20 Divider Flujos'],
    'flujos':      ['21 Flujos Resumen'],
    'maquinaria':  ['26 Flujo Maquinaria'],
    'detalle':     ['22 Detalle Movimientos'],
    'div-pcv':     ['22 Divider PCV'],
    'pcv':         ['23 PCV Cumplimiento'],
    'revision':    ['24 Registros a Revisar'],
    'cxp':         ['29 Divider CXP'],
    'cxp-det':     ['30 CXP Cuentas por Pagar'],
    'anexos':      ['31 Divider Anexos','32 Indice Anexos'],
    'cierre':      ['33 Cierre'],
  };
  document.querySelectorAll('.slide').forEach(s => {
    const label = s.dataset.screenLabel || '';
    // Find which config id this slide belongs to
    for (const [cfgId, labels] of Object.entries(idToLabel)) {
      if (labels.some(l => label.startsWith(l.split(' ')[0]) && label.includes(l.split(' ')[1]||''))) {
        if (config[cfgId] === false) {
          s.dataset.hidden = 'true';
          s.style.display = 'none';
        } else {
          delete s.dataset.hidden;
          s.style.display = '';
        }
        break;
      }
    }
    // Also handle dividers - hide if their section is hidden
    if (label.includes('Divider')) {
      const sec = label.toLowerCase();
      let shouldHide = false;
      if (sec.includes('inventario') && config['inv-consol'] === false && config['absorcion'] === false && config['valor'] === false) shouldHide = true;
      if (sec.includes('ventas') && config['ventas-res'] === false && config['tendencia'] === false) shouldHide = true;
      if (sec.includes('cartera') && config['cartera-res'] === false) shouldHide = true;
      if (sec.includes('flujos') && config['flujos'] === false && config['detalle'] === false) shouldHide = true;
      if (sec.includes('pcv') && config['pcv'] === false && config['revision'] === false) shouldHide = true;
      if (shouldHide) { s.dataset.hidden = 'true'; s.style.display = 'none'; }
    }
  });
}

/* ── Init ───────────────────────────────────────── */
function init() {
  // Theme
  applyTheme(localStorage.getItem('rv4_pres_theme') || 'light');
  document.getElementById('themeBtn').addEventListener('click', toggleTheme);

  // Period selectors
  document.getElementById('periodMes').value = state.mes;
  document.getElementById('periodAnio').value = state.anio;
  // Debounced period change — prevents race conditions when user switches filters rapidly
  let _loadAllTimer = null;
  let _loadAllVersion = 0; // increments on each loadAll call; async callbacks check this
  function scheduledLoadAll() {
    clearTimeout(_loadAllTimer);
    _loadAllTimer = setTimeout(() => { _loadAllVersion++; loadAll(); }, 300);
  }
  document.getElementById('periodMes').addEventListener('change', e => { state.mes  = Number(e.target.value); scheduledLoadAll(); });
  document.getElementById('periodAnio').addEventListener('change', e => { state.anio = Number(e.target.value); scheduledLoadAll(); });
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
  document.getElementById('tendenciaProyecto')?.addEventListener('change', () => { if (getToken()) reloadTendencia(); });

  // SSO from URL (?token=xxx&usuario=base64) + período (?mes=X&anio=X)
  const params = new URLSearchParams(window.location.search);
  if (params.get('token')) {
    localStorage.setItem('token', params.get('token'));
    if (params.get('usuario')) {
      try { localStorage.setItem('usuario', atob(params.get('usuario'))); } catch(e) {}
    }
  }

  // Aplicar período de URL si viene (ej. desde jd.html → /presentacion.html?mes=5&anio=2026)
  const urlMes  = params.get('mes');
  const urlAnio = params.get('anio');
  if (urlMes !== null && urlMes !== undefined) state.mes = Number(urlMes);
  if (urlAnio)                                  state.anio = Number(urlAnio);

  // Si no vino período, usar año actual con "todo el año" (Junta = balance anual)
  if (!urlMes && !urlAnio) {
    const now = new Date();
    state.anio = now.getFullYear();
    state.mes  = 0;
  }
  // Reflejar en los selectores
  document.getElementById('periodAnio').value = state.anio;
  document.getElementById('periodMes').value  = state.mes;

  // Limpiar query string para no exponer el token
  if (params.get('token') || urlMes || urlAnio) {
    history.replaceState({}, '', window.location.pathname);
  }

  fitStage();
  setTimeout(fitStage, 100);
  showSlide(0);
  loadAll();
}

document.addEventListener('DOMContentLoaded', () => { applySlideConfig(); }, { once: true });
document.addEventListener('DOMContentLoaded', init);
