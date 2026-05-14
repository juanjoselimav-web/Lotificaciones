/* ═══════════════════════════════════════════════════════════════
   RV4 — ANEXOS de la Presentación Junta Directiva
   Extiende presentacion.js · NO modifica el resto de la presentación.

   Agrega 15 slides al final (antes del Gracias):
     · 1 Divider "Sección 07 — Anexos"
     · 1 Índice con grid de los 13 proyectos (clic = ir al anexo)
     · 13 fichas ejecutivas (1 por proyecto)

   Envuelto en IIFE para evitar colisiones (fmtQ, fmtQM, etc.) con presentacion.js
   ═══════════════════════════════════════════════════════════════ */
(function(){

const PROYECTOS_ANEXO = [
  { proyecto_id:1,  nombre:'Hacienda Jumay',              sociedad:'EFICIENCIA URBANA',              sociedadNombre:'Eficiencia Urbana',          logo:'logos/hacienda-jumay.png' },
  { proyecto_id:2,  nombre:'La Ceiba',                    sociedad:'SER GEN CCC',        sociedadNombre:'Servicios Generales',        sociedadAlt:'SERVICIOS GENERALES CCC', logo:'logos/condado-la-ceiba.png' },
  { proyecto_id:3,  nombre:'Hacienda el Sol',             sociedad:'ROSSIO',                         sociedadNombre:'Rossio',                     logo:'logos/hacienda-sol.png' },
  { proyecto_id:4,  nombre:'Oasis Zacapa',                sociedad:'FRUGALEX',                       sociedadNombre:'Frugalex',                   logo:'logos/oasis-zacapa.png' },
  { proyecto_id:5,  nombre:'Cañadas de Jalapa',           sociedad:'OTTAVIA',                        sociedadNombre:'Ottavia',                    logo:'logos/canadas-jalapa.png' },
  { proyecto_id:6,  nombre:'Condado Jutiapa',             sociedad:'UTILICA',                        sociedadNombre:'Utilica',                    logo:'logos/condado-jutiapa.png' },
  { proyecto_id:7,  nombre:'Club Campestre Jumay',        sociedad:'TEZZOLI',                        sociedadNombre:'Tezzoli',                    logo:'logos/club-campestre-jumay.png' },
  { proyecto_id:8,  nombre:'Club del Bosque',             sociedad:'URBIVA 2',                       sociedadNombre:'Urbiva 2',                   sociedadAlt:'Urviba 2', logo:'logos/club-del-bosque.png' },
  { proyecto_id:9,  nombre:'Club Residencial Progreso',   sociedad:'GARBATELLA',                     sociedadNombre:'Garbatella',                 logo:'logos/club-residencial-progreso.png' },
  { proyecto_id:10, nombre:'Arboleda Santa Elena',        sociedad:'CAPIPOS',                        sociedadNombre:'Capipos',                    logo:'logos/arboleda-santa-elena.png' },
  { proyecto_id:11, nombre:'Hacienda Santa Lucia',        sociedad:'OVEST',                          sociedadNombre:'Ovest',                      logo:'logos/hacienda-santa-lucia.png' },
  { proyecto_id:12, nombre:'Hacienda El Cafetal Fase I',  sociedad:'CORCOLLE',                       sociedadNombre:'Corcolle',                   logo:'logos/hacienda-cafetal.png' },
  { proyecto_id:13, nombre:'Hacienda El Cafetal Fase III',sociedad:'GIBRALEON',                      sociedadNombre:'Gibraleon',                  logo:'logos/hacienda-cafetal.png' }
];

// Estado de los datos por proyecto (cache)
const anexosData = {};

// ── Inyecta los slides en el DOM ────────────────────
function buildAnexosSlides() {
  const deck = document.getElementById('deck');
  if (!deck) return;
  // Cierre = última slide existente (slide 25 — Gracias). Insertamos antes.
  const cierre = deck.querySelector('section.slide[data-screen-label="25 Cierre"]');
  if (!cierre) return;

  // El número de slide para "Anexos" parte después del slide 24 (PCV registros).
  // El cierre se renumera al final dinámicamente.
  const total = 24 + 2 + PROYECTOS_ANEXO.length + 1; // 24 base + divider + indice + N proy + cierre

  const html = [];

  // ── Divider Anexos ────────────────────────────────
  const dividerNum = 25;
  html.push(`
    <section class="slide" data-screen-label="${String(dividerNum).padStart(2,'0')} Divider Anexos">
      <div class="divider">
        <div class="divider-num">07</div>
        <div class="divider-content">
          <div class="divider-rule"></div>
          <span class="divider-eyebrow">Sección 07</span>
          <h2 class="divider-title">Anexos</h2>
          <p class="divider-desc">Fichas ejecutivas por proyecto · indicadores consolidados de inventario, ventas, cartera y flujo. Disponibles para consulta y profundización durante la sesión.</p>
        </div>
      </div>
    </section>`);

  // ── Índice de Anexos ──────────────────────────────
  const indiceNum = dividerNum + 1;
  const indiceCards = PROYECTOS_ANEXO.map((p, i) => {
    const slideNum = indiceNum + 1 + i;
    return `<div class="ax-card" onclick="goToAnexo(${i})">
      <div class="ax-card-logo"><img src="${p.logo}" alt="${p.nombre}" onerror="this.style.display='none'"></div>
      <div class="ax-card-body">
        <div class="ax-card-name">${p.nombre}</div>
        <div class="ax-card-sociedad">${p.sociedadNombre}</div>
      </div>
      <div class="ax-card-num">07.${String(i+1).padStart(2,'0')}</div>
    </div>`;
  }).join('');

  html.push(`
    <section class="slide" data-screen-label="${String(indiceNum).padStart(2,'0')} Indice Anexos" id="slideIndiceAnexos">
      <div class="slide-pad">
        <div class="slide-header">
          <div class="slide-header-left">
            <span class="slide-section-tag">07 · Anexos</span>
            <h2 class="slide-title">Índice de proyectos</h2>
            <p class="slide-subtitle">${PROYECTOS_ANEXO.length} proyectos · clic en cualquier tarjeta para abrir su ficha</p>
          </div>
          <div class="slide-header-right"><span class="slide-num">${indiceNum} / ${total}</span></div>
        </div>
        <div class="ax-grid">${indiceCards}</div>
      </div>
      <div class="slide-footer"><span>Anexos · Índice de proyectos</span><span class="meta-period" data-period></span></div>
    </section>`);

  // ── Ficha por proyecto ────────────────────────────
  PROYECTOS_ANEXO.forEach((p, i) => {
    const slideNum = indiceNum + 1 + i;
    const labelNum = String(slideNum).padStart(2,'0');
    const codigo = `07.${String(i+1).padStart(2,'0')}`;
    html.push(`
      <section class="slide ax-slide" data-screen-label="${labelNum} Anexo ${p.nombre}" data-anexo-idx="${i}">
        <div class="slide-pad">
          <button class="ax-back" onclick="goToIndiceAnexos()" title="Volver al índice de proyectos">← Volver al índice</button>
          <div class="slide-header ax-header">
            <div class="slide-header-left">
              <span class="slide-section-tag">Anexo ${codigo} · ${p.sociedadNombre}</span>
              <h2 class="slide-title">${p.nombre}</h2>
              <p class="slide-subtitle" id="ax-${i}-sub">Cargando indicadores…</p>
            </div>
            <div class="ax-header-right">
              <div class="ax-logo-wrap"><img src="${p.logo}" alt="${p.nombre}" onerror="this.parentElement.innerHTML='<div class=\\'ax-logo-fallback\\'>${initials(p.nombre)}</div>'"></div>
              <span class="slide-num">${slideNum} / ${total}</span>
            </div>
          </div>

          <div class="ax-quadrants">
            <!-- INVENTARIO -->
            <div class="ax-q">
              <div class="ax-q-head"><span class="ax-q-title">📦 Inventario</span><span class="ax-q-pill blue">Al corte de hoy</span></div>
              <div class="ax-q-body" id="ax-${i}-inv"><div class="ax-skel"></div></div>
            </div>
            <!-- VENTAS -->
            <div class="ax-q">
              <div class="ax-q-head"><span class="ax-q-title">💰 Ventas</span><span class="ax-q-pill amber ax-q-period">Período</span></div>
              <div class="ax-q-body" id="ax-${i}-ven"><div class="ax-skel"></div></div>
            </div>
            <!-- CARTERA -->
            <div class="ax-q">
              <div class="ax-q-head"><span class="ax-q-title">📊 Cartera</span><span class="ax-q-pill amber ax-q-period">Período</span></div>
              <div class="ax-q-body" id="ax-${i}-car"><div class="ax-skel"></div></div>
            </div>
            <!-- FLUJO -->
            <div class="ax-q">
              <div class="ax-q-head"><span class="ax-q-title">💵 Flujo de efectivo</span><span class="ax-q-pill amber ax-q-period">Período</span></div>
              <div class="ax-q-body" id="ax-${i}-flu"><div class="ax-skel"></div></div>
            </div>
          </div>

          <div class="ax-lectura" id="ax-${i}-lect">
            <span class="ax-lectura-label">Lectura ejecutiva</span>
            <span class="ax-lectura-text">Generando lectura…</span>
          </div>
        </div>
        <div class="slide-footer"><span>Anexo ${codigo} · ${p.nombre}</span><span class="meta-period" data-period></span></div>
      </section>`);
  });

  // Insertamos todo el bloque antes del slide de Cierre
  const wrap = document.createElement('div');
  wrap.innerHTML = html.join('');
  while (wrap.firstChild) {
    cierre.parentNode.insertBefore(wrap.firstChild, cierre);
  }

  // Renumera el slide de cierre (ya quedó en la posición correcta)
  const cierreNum = cierre.querySelector('.slide-num');
  if (cierreNum) cierreNum.textContent = `${total} / ${total}`;
  // Actualiza el contador top-left para reflejar el nuevo total
  if (window.state) window.state.total = total;
}

function initials(nombre) {
  return nombre.split(' ').slice(0,2).map(s=>s[0]).join('').toUpperCase();
}

// ── Navegación helpers ──────────────────────────────
window.goToIndiceAnexos = function() {
  const slides = document.querySelectorAll('.slide');
  const idx = [...slides].findIndex(s => s.id === 'slideIndiceAnexos');
  if (idx >= 0) window.showSlide(idx);
};
window.goToAnexo = function(i) {
  const slides = document.querySelectorAll('.slide');
  const idx = [...slides].findIndex(s => s.dataset.anexoIdx === String(i));
  if (idx >= 0) window.showSlide(idx);
};

// ── Cargar datos por proyecto ───────────────────────
async function loadAnexoData(idx) {
  const p = PROYECTOS_ANEXO[idx];
  if (!p) return;

  const invItem = (window.state?.data?.inventario?.proyectos || []).find(
    x => (x.nombre_proyecto || '').toLowerCase() === p.nombre.toLowerCase()
  );

  let ventas = null, cartera = null, flujo = null;

  const token = localStorage.getItem('token');
  if (token) {
    // ── Ventas: parámetro "proyecto" por nombre ──────
    const vqs = new URLSearchParams();
    vqs.set('año', window.state.anio);
    if (window.state.mes > 0) vqs.set('mes', window.state.mes);
    vqs.set('proyecto', p.nombre);
    ventas = await safeFetch(`/api/ventas/kpis?${vqs}`);
    await new Promise(ok => setTimeout(ok, 200));

    // ── Cartera: pasar año+mes para datos del período filtrado ──
    const cqs = `año=${window.state.anio}${window.state.mes > 0 ? '&mes=' + window.state.mes : ''}`;
    // Intentar con sociedadNombre, luego con sociedad (mayúsculas), luego sin empresa
    cartera = await safeFetch(`/api/cartera/kpis?empresa=${encodeURIComponent(p.sociedadNombre)}&${cqs}`);
    if (!cartera || !cartera.cartera_total) {
      cartera = await safeFetch(`/api/cartera/kpis?empresa=${encodeURIComponent(p.sociedad)}&${cqs}`);
    }
    await new Promise(ok => setTimeout(ok, 200));

    // ── Flujos: intentar todas las variaciones del nombre de sociedad ──
    const socActual = resolverSociedad(p);
    const socVariants = [socActual, p.sociedad, p.sociedadNombre];
    if (p.sociedadAlt) socVariants.push(p.sociedadAlt);
    // Add common transformations
    socVariants.push(p.sociedad.replace(/ CCC$/, ''), p.sociedad.replace(/ 2$/, ''));
    const uniqueVars = [...new Set(socVariants)].filter(Boolean);
    for (const soc of uniqueVars) {
      const r = await safeFetch(`/api/flujos/resumen?sociedad=${encodeURIComponent(soc)}&granularidad=mes`);
      if (r && r.periodos?.length) { flujo = r; break; }
    }
  } else {
    [ventas, cartera, flujo] = derivarDemoProyecto(p, idx);
  }

  anexosData[idx] = { inv: invItem, ven: ventas, car: cartera, flu: flujo };
  renderAnexo(idx);
}

async function safeFetch(path, retries = 1) {
  const token = localStorage.getItem('token');
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const r = await fetch(path, { headers: { 'Authorization': `Bearer ${token}` } });
      if (r.status === 503 && attempt < retries) {
        await new Promise(ok => setTimeout(ok, 1500)); // wait 1.5s before retry
        continue;
      }
      if (!r.ok) return null;
      return r.json();
    } catch(e) {
      if (attempt < retries) { await new Promise(ok => setTimeout(ok, 1500)); continue; }
      return null;
    }
  }
  return null;
}

// ── Render ──────────────────────────────────────────
function renderAnexo(idx) {
  const p = PROYECTOS_ANEXO[idx];
  const d = anexosData[idx] || {};
  const inv = d.inv || {};
  const ven = d.ven || {};
  const car = d.car || {};
  const flu = d.flu || {};

  const periodo = window.periodoFormal ? window.periodoFormal() : '';
  setT(`ax-${idx}-sub`, `Indicadores ejecutivos · Inventario al corte · resto del período: ${periodo}`);

  // Actualiza los pills de período en este slide
  document.querySelectorAll(`[data-anexo-idx="${idx}"] .ax-q-period`).forEach(el => el.textContent = periodo);

  // ─ INVENTARIO ─
  const totalLotes = Number(inv.total_lotes || 0);
  const dispLotes  = Number(inv.disponibles || 0);
  const vendLotes  = Number(inv.vendidos_reservados || inv.vendidos || 0);
  const abs        = Number(inv.porcentaje_absorcion || 0);
  const valDisp    = Number(inv.valor_disponible || 0);
  const valTotal   = Number(inv.valor_total || 0);
  setH(`ax-${idx}-inv`, !totalLotes ? sinDatos() : `
    <div class="ax-metric-row">
      <div class="ax-metric"><span class="ax-metric-v">${fmt(totalLotes)}</span><span class="ax-metric-l">Lotes totales</span></div>
      <div class="ax-metric"><span class="ax-metric-v green">${fmt(dispLotes)}</span><span class="ax-metric-l">Disponibles</span></div>
      <div class="ax-metric"><span class="ax-metric-v blue">${fmt(vendLotes)}</span><span class="ax-metric-l">Vendidos</span></div>
    </div>
    <div class="ax-bar-block">
      <div class="ax-bar-label"><span>Absorción</span><strong>${abs.toFixed(1)}%</strong></div>
      <div class="ax-bar"><div class="ax-bar-fill" style="width:${Math.min(abs,100)}%"></div></div>
    </div>
    <div class="ax-foot">Valor disponible: <strong>${fmtQM(valDisp)}</strong> · Total: ${fmtQM(valTotal)}</div>
  `);

  // ─ VENTAS ─
  const vBrutas = ven.ventas_brutas, vNetas = ven.ventas_netas;
  setH(`ax-${idx}-ven`, vBrutas == null ? sinDatos('Sin datos en el período · proyecto sin movimiento o endpoint no filtra por proyecto') : `
    <div class="ax-metric-row">
      <div class="ax-metric"><span class="ax-metric-v blue">${fmt(ven.ventas_brutas)}</span><span class="ax-metric-l">Brutas</span></div>
      <div class="ax-metric"><span class="ax-metric-v red">${fmt(ven.desistimientos)}</span><span class="ax-metric-l">Desistim.</span></div>
      <div class="ax-metric"><span class="ax-metric-v green">${fmt(ven.ventas_netas)}</span><span class="ax-metric-l">Netas</span></div>
    </div>
    <div class="ax-mini-mezcla">
      ${miniBar('Contado', ven.contado, vBrutas, 'var(--green)')}
      ${miniBar('Crédito s/int', ven.sin_interes, vBrutas, 'var(--blue)')}
      ${miniBar('Crédito c/int', ven.con_interes, vBrutas, 'var(--dorado)')}
    </div>
    <div class="ax-foot">Ticket promedio: <strong>${fmtQ(ven.ticket_promedio)}</strong> · Valor bruto: ${fmtQM(ven.valor_bruto)}</div>
  `);

  // ─ CARTERA ─
  setH(`ax-${idx}-car`, !car.cartera_total ? sinDatos('Sin datos de cartera para este proyecto') : `
    <div class="ax-metric-row">
      <div class="ax-metric"><span class="ax-metric-v">${fmtQM(car.cartera_total)}</span><span class="ax-metric-l">Cartera total</span></div>
      <div class="ax-metric"><span class="ax-metric-v amber">${(car.tasa_mora||0).toFixed(1)}%</span><span class="ax-metric-l">Tasa mora</span></div>
      <div class="ax-metric"><span class="ax-metric-v">${fmt(car.clientes_activos)}</span><span class="ax-metric-l">Clientes</span></div>
    </div>
    <div class="ax-mini-aging">
      <div class="ax-row"><span>Capital</span><strong class="blue">${fmtQM(car.capital_total)}</strong></div>
      <div class="ax-row"><span>Intereses</span><strong class="purple">${fmtQM(car.intereses_total)}</strong></div>
      <div class="ax-row"><span>Mora total</span><strong class="red">${fmtQM(car.mora_total)}</strong></div>
    </div>
    <div class="ax-foot">Cobro próx. 30 días: <strong>${fmtQM(car.cobro_30d)}</strong></div>
  `);

  // ─ FLUJO ─
  let flujoTarget = null, saldoIni=0, saldoFin=0, totalIng=0, totalEgr=0;
  if (flu && flu.periodos && flu.periodos.length) {
    let target = flu.periodos[flu.periodos.length-1];
    if (window.state.mes > 0) {
      const cand = `${window.state.anio}-${String(window.state.mes).padStart(2,'0')}`;
      if (flu.periodos.includes(cand)) target = cand;
    } else {
      const yr = flu.periodos.filter(x => x.startsWith(String(window.state.anio)));
      if (yr.length) target = yr[yr.length-1];
    }
    flujoTarget = target;
    saldoIni = flu.saldos_iniciales?.[target] || 0;
    saldoFin = flu.saldos_finales?.[target]   || 0;
    for (const sec of (flu.secciones||[])) {
      const t = sec.totales?.[target]; if (!t) continue;
      totalIng += t.ingreso || 0; totalEgr += t.egreso || 0;
    }
  }
  const neto = totalIng - totalEgr;
  setH(`ax-${idx}-flu`, !flujoTarget ? sinDatos(`Sin flujo registrado · intentado con: ${p.sociedad}${p.sociedadNombre !== p.sociedad ? ' / '+p.sociedadNombre : ''}`) : `
    <div class="ax-metric-row">
      <div class="ax-metric"><span class="ax-metric-v green">${fmtQM(totalIng)}</span><span class="ax-metric-l">Ingresos</span></div>
      <div class="ax-metric"><span class="ax-metric-v red">${fmtQM(totalEgr)}</span><span class="ax-metric-l">Egresos</span></div>
      <div class="ax-metric"><span class="ax-metric-v ${neto>=0?'green':'red'}">${neto>=0?'+':''}${fmtQM(neto)}</span><span class="ax-metric-l">Neto</span></div>
    </div>
    <div class="ax-mini-aging">
      <div class="ax-row"><span>Saldo inicial</span><strong>${fmtQM(saldoIni)}</strong></div>
      <div class="ax-row"><span>Saldo final</span><strong class="dorado">${fmtQM(saldoFin)}</strong></div>
    </div>
    <div class="ax-foot">Período: <strong>${flujoTarget}</strong> · Sociedad: ${p.sociedadNombre}</div>
  `);

  // ─ Lectura ejecutiva ─
  setLectura(idx, generarLectura(p, inv, ven, car, neto));
}

function generarLectura(p, inv, ven, car, fluNeto) {
  const partes = [];
  const abs = Number(inv.porcentaje_absorcion || 0);
  if (abs >= 60) partes.push('proyecto con absorción alta');
  else if (abs >= 30) partes.push('absorción en rango medio');
  else if (abs > 0) partes.push('absorción baja, gran inventario disponible');

  if (ven.ventas_brutas != null && ven.ventas_brutas > 0) {
    partes.push(`${ven.ventas_brutas} ventas en el período`);
    if (ven.desistimientos > 0) {
      const tasa = (ven.desistimientos / ven.ventas_brutas * 100).toFixed(0);
      if (tasa > 15) partes.push(`tasa de desistimiento elevada (${tasa}%)`);
    }
  }

  const mora = Number(car.tasa_mora || 0);
  if (mora > 15) partes.push(`mora crítica (${mora.toFixed(1)}%) — atención prioritaria`);
  else if (mora > 10) partes.push(`mora por encima del objetivo (${mora.toFixed(1)}%)`);
  else if (mora > 0) partes.push(`cartera sana (mora ${mora.toFixed(1)}%)`);

  if (fluNeto > 0) partes.push('flujo neto positivo');
  else if (fluNeto < 0) partes.push('flujo neto negativo en el período');

  if (!partes.length) return 'Sin datos suficientes para generar lectura — verificar período seleccionado.';
  return capitalize(partes.join(' · ')) + '.';
}
function capitalize(s){ return s.charAt(0).toUpperCase() + s.slice(1); }

function setLectura(idx, txt) {
  const el = document.getElementById(`ax-${idx}-lect`);
  if (el) el.querySelector('.ax-lectura-text').textContent = txt;
}

// ── helpers de formato (reusan los globales si existen) ──
function fmt(n)   { return Number(n||0).toLocaleString('es-GT'); }
function fmtQ(n)  { return 'Q ' + Number(n||0).toLocaleString('es-GT',{maximumFractionDigits:0}); }
function fmtQM(n) { const v=Number(n||0); if(Math.abs(v)>=1e6) return 'Q '+(v/1e6).toFixed(1)+'M'; if(Math.abs(v)>=1e3) return 'Q '+(v/1e3).toFixed(0)+'K'; return 'Q '+v.toFixed(0); }
function setT(id,v){ const el=document.getElementById(id); if(el) el.textContent=v; }
function setH(id,v){ const el=document.getElementById(id); if(el) el.innerHTML=v; }
function sinDatos(msg){ return `<div class="ax-empty">${msg||'Sin datos disponibles en el período'}</div>`; }
function miniBar(label, v, total, color) {
  const p = total ? (Number(v||0)/total*100) : 0;
  return `<div class="ax-mini-bar-row">
    <span class="ax-mini-bar-lbl"><span class="ax-dot" style="background:${color}"></span>${label}</span>
    <div class="ax-mini-bar"><div style="background:${color};width:${p}%"></div></div>
    <span class="ax-mini-bar-v">${fmt(v)}</span>
  </div>`;
}

// ── Datos demo derivados (cuando no hay token) ──
function derivarDemoProyecto(p, idx) {
  // Ventas demo: distribuir el total demo entre los 13 proyectos proporcional al inventario
  const inv = window.state?.data?.inventario;
  if (!inv) return [null, null, null];
  const item = inv.proyectos.find(x => x.proyecto_id === p.proyecto_id) || {};
  const totalLotes = inv.totales.total_lotes || 1;
  const peso = (item.total_lotes || 0) / totalLotes;

  const vk = window.state.data.ventas?.k || {};
  const ck = window.state.data.cartera?.k || {};

  const ventas = {
    ventas_brutas: Math.round((vk.ventas_brutas||0) * peso),
    desistimientos: Math.round((vk.desistimientos||0) * peso),
    ventas_netas: Math.round((vk.ventas_netas||0) * peso),
    valor_bruto: Math.round((vk.valor_bruto||0) * peso),
    ticket_promedio: vk.ticket_promedio || 0,
    contado: Math.round((vk.contado||0) * peso),
    sin_interes: Math.round((vk.sin_interes||0) * peso),
    con_interes: Math.round((vk.con_interes||0) * peso)
  };
  const cartera = {
    cartera_total: (ck.cartera_total||0) * peso,
    capital_total: (ck.capital_total||0) * peso,
    intereses_total: (ck.intereses_total||0) * peso,
    mora_total: (ck.mora_total||0) * peso,
    tasa_mora: (ck.tasa_mora||0) + (Math.random()*6 - 3), // pequeña variación demo
    clientes_activos: Math.round((ck.clientes_activos||0) * peso),
    cobro_30d: (ck.cobro_30d||0) * peso
  };
  // Flujo demo
  const ingDemo = 800000 + Math.random()*2400000;
  const egrDemo = ingDemo * (0.6 + Math.random()*0.3);
  const flujo = {
    periodos: [`${window.state.anio}-${String(Math.max(window.state.mes,1)).padStart(2,'0')}`],
    saldos_iniciales: {}, saldos_finales: {},
    secciones: [
      { seccion:'INGRESOS', totales:{} },
      { seccion:'EGRESOS / URBANIZACION', totales:{} },
      { seccion:'EGRESOS / ADMINISTRACION', totales:{} }
    ]
  };
  const tgt = flujo.periodos[0];
  flujo.saldos_iniciales[tgt] = 800000 + idx*120000;
  flujo.saldos_finales[tgt]   = flujo.saldos_iniciales[tgt] + (ingDemo - egrDemo);
  flujo.secciones[0].totales[tgt] = { ingreso: ingDemo, egreso: 0, neto: ingDemo };
  flujo.secciones[1].totales[tgt] = { ingreso: 0, egreso: egrDemo*0.55, neto: -egrDemo*0.55 };
  flujo.secciones[2].totales[tgt] = { ingreso: 0, egreso: egrDemo*0.45, neto: -egrDemo*0.45 };
  return [ventas, cartera, flujo];
}

// ── Cache de sociedades válidas del flujos API ──────
let _sociedadesValidas = null;
async function discoverSociedades() {
  if (_sociedadesValidas) return;
  try {
    const d = await safeFetch('/api/flujos/resumen?sociedad=CONSOLIDADO&granularidad=mes');
    if (d?.secciones) {
      // Build lookup from main presentation dropdown if available
      const sel = document.getElementById('flujoSociedad');
      if (sel) {
        _sociedadesValidas = [...sel.options].map(o => o.value).filter(v => v && v !== 'CONSOLIDADO');
      }
    }
    // Also try fetching the list of sociedades
    if (!_sociedadesValidas?.length) {
      const inv = window.state?.data?.inventario?.proyectos || [];
      _sociedadesValidas = inv.map(p => p.nombre_sociedad).filter(Boolean);
    }
  } catch(e) {}
}

function resolverSociedad(p) {
  if (!_sociedadesValidas?.length) return p.sociedad;
  // Try exact match first
  if (_sociedadesValidas.includes(p.sociedad)) return p.sociedad;
  // Try case-insensitive match
  const lp = p.sociedad.toLowerCase();
  const match = _sociedadesValidas.find(s => s.toLowerCase() === lp || s.toLowerCase().includes(lp.split(' ')[0].toLowerCase()));
  return match || p.sociedad;
}

// ── Recarga de anexos cuando cambia el período (secuencial + delays) ──
async function reloadAnexos() {
  await discoverSociedades();
  for (let i = 0; i < PROYECTOS_ANEXO.length; i++) {
    await loadAnexoData(i);
    await new Promise(ok => setTimeout(ok, 400));
  }
}

// ── Init ─────────────────────────────────────────────
function initAnexos() {
  buildAnexosSlides();
  // Esperar 5s a que la presentación principal termine TODAS sus peticiones
  setTimeout(() => reloadAnexos(), 5000);

  // Re-render cuando el usuario cambie el período
  document.getElementById('periodMes')?.addEventListener('change',  () => setTimeout(reloadAnexos, 1500));
  document.getElementById('periodAnio')?.addEventListener('change', () => setTimeout(reloadAnexos, 1500));
}

// Espera DOM + presentacion.js
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initAnexos);
} else {
  initAnexos();
}

})();
