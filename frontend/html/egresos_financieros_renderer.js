
/* ═══════════════════════════════════════════════════════════════
   SECCIÓN 3 — EGRESOS FINANCIEROS
   Muestra: préstamo bancario (tabla amortización), ejecutado vs tabla,
   intercompany, y proyección de cuotas por año.
   ═══════════════════════════════════════════════════════════════ */

async function renderEgresosFinancieros(empresa) {
  const container = document.getElementById('seccion-egresos-financieros');
  if (!container) return;

  container.innerHTML = `
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
      <div class="spinner" style="width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--dorado);border-radius:50%;animation:spin .7s linear infinite"></div>
      <span style="color:var(--muted);font-size:13px">Cargando egresos financieros…</span>
    </div>`;

  try {
    const data = await apiFetch(`/api/proyecciones/${encodeURIComponent(empresa)}/egresos-financieros`);
    const prest = data.prestamo_bancario;
    const ic    = data.intercompany || {};
    const ejec  = data.ejecutado_financiamiento || {};
    const porAnio = data.cuotas_por_anio || {};

    if (!prest) {
      container.innerHTML = `
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:24px;text-align:center;color:var(--muted)">
          <div style="font-size:24px;margin-bottom:8px">🏦</div>
          <div style="font-weight:600;margin-bottom:4px">Sin préstamo bancario registrado</div>
          <div style="font-size:12px">Este proyecto no tiene tabla de amortización en el sistema</div>
        </div>`;
      return;
    }

    const fmtQ  = v => 'Q ' + Number(v||0).toLocaleString('es-GT', {minimumFractionDigits:0, maximumFractionDigits:0});
    const fmtQM = v => { const n=Number(v||0); if(Math.abs(n)>=1e6) return 'Q '+(n/1e6).toFixed(2)+'M'; if(Math.abs(n)>=1e3) return 'Q '+(n/1e3).toFixed(0)+'K'; return 'Q '+n.toFixed(0); };
    const pct   = (a,b) => b ? (a/b*100).toFixed(1)+'%' : '—';
    const hoy   = new Date().toISOString().split('T')[0];

    // ── KPI summary bar ──────────────────────────────────────────
    const totalPrestamo = prest.monto_original || 0;
    const capitalPagado = prest.pagado_capital || 0;
    const interesPagado = prest.pagado_interes || 0;
    const totalPagado   = prest.pagado_total   || 0;
    const capPend       = prest.pendiente_capital || 0;
    const intPend       = prest.pendiente_interes || 0;
    const totPend       = prest.pendiente_total   || 0;
    const nCuotasPag    = prest.cuotas_pagadas || 0;
    const cuotasPend    = prest.cuotas_pendientes || [];
    const pctCapPagado  = totalPrestamo ? capitalPagado/totalPrestamo*100 : 0;

    // Real vs tabla comparison
    const ejec_total  = ejec.total || 0;
    const ejec_cats   = ejec.por_categoria || {};

    // Intercompany
    const icSaldo = ic.saldo || 0;
    const icTipo  = ic.tipo  || 'por pagar';

    const kpiHtml = `
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px">
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;border-top:3px solid var(--dorado)">
          <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px">Préstamo original</div>
          <div style="font-size:22px;font-weight:800;color:var(--dorado)">${fmtQM(totalPrestamo)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${prest.banco} · ${prest.tasa*100}% anual</div>
        </div>
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;border-top:3px solid #2c7a4e">
          <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px">Capital pagado</div>
          <div style="font-size:22px;font-weight:800;color:#2c7a4e">${fmtQM(capitalPagado)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${pct(capitalPagado,totalPrestamo)} del total · ${nCuotasPag} cuotas</div>
        </div>
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;border-top:3px solid #e05050">
          <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px">Capital pendiente</div>
          <div style="font-size:22px;font-weight:800;color:#e05050">${fmtQM(capPend)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${pct(capPend,totalPrestamo)} del total · ${cuotasPend.length} cuotas</div>
        </div>
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;border-top:3px solid #e0a030">
          <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px">Intereses totales</div>
          <div style="font-size:22px;font-weight:800;color:#e0a030">${fmtQM(interesPagado + intPend)}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">Pagado: ${fmtQM(interesPagado)} · Pendiente: ${fmtQM(intPend)}</div>
        </div>
      </div>`;

    // ── Barra de avance ──────────────────────────────────────────
    const progressHtml = `
      <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 18px;margin-bottom:14px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
          <span style="font-size:12px;font-weight:700;color:var(--text)">Avance de amortización</span>
          <span style="font-size:12px;font-weight:700;color:var(--dorado)">${pct(capitalPagado,totalPrestamo)} capital amortizado</span>
        </div>
        <div style="background:rgba(255,255,255,.05);border-radius:6px;height:10px;overflow:hidden">
          <div style="background:linear-gradient(90deg,#2c7a4e,#4aaa6e);height:100%;width:${Math.min(pctCapPagado,100)}%;border-radius:6px;transition:width .6s"></div>
        </div>
        <div style="display:flex;justify-content:space-between;margin-top:4px;font-size:10px;color:var(--muted)">
          <span>Pagado: ${fmtQ(capitalPagado)}</span>
          <span>Pendiente: ${fmtQ(capPend)}</span>
        </div>
      </div>`;

    // ── Comparación ejecutado real vs tabla ──────────────────────
    // ejec_cats contains: Prestamo Bancario, Intercompany, Comisiones Bancarias, etc.
    const ejec_prest = ejec_cats['Prestamo Bancario'] || 0;
    const diferencia = ejec_prest - totalPagado;
    const difColor   = Math.abs(diferencia) < 1000 ? '#2c7a4e' : diferencia > 0 ? '#e0a030' : '#e05050';
    const difLabel   = Math.abs(diferencia) < 1000 ? '✓ Cuadra' : diferencia > 0 ? '⚠ Flujo mayor' : '⚠ Flujo menor';

    const cuadreHtml = `
      <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 18px;margin-bottom:14px">
        <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:10px">
          Ejecutado real vs tabla de amortización
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px">
          <div style="text-align:center;padding:10px;background:rgba(255,255,255,.03);border-radius:6px">
            <div style="font-size:10px;color:var(--muted);margin-bottom:4px">Flujo real (SAP)</div>
            <div style="font-size:16px;font-weight:700;color:var(--text)">${fmtQ(ejec_prest)}</div>
            <div style="font-size:10px;color:var(--muted);margin-top:2px">FINANCIAMIENTO · Prestamo</div>
          </div>
          <div style="text-align:center;padding:10px;background:rgba(255,255,255,.03);border-radius:6px">
            <div style="font-size:10px;color:var(--muted);margin-bottom:4px">Tabla amortización</div>
            <div style="font-size:16px;font-weight:700;color:var(--text)">${fmtQ(totalPagado)}</div>
            <div style="font-size:10px;color:var(--muted);margin-top:2px">${nCuotasPag} cuotas procesadas</div>
          </div>
          <div style="text-align:center;padding:10px;background:rgba(255,255,255,.03);border-radius:6px;border:1px solid ${difColor}40">
            <div style="font-size:10px;color:var(--muted);margin-bottom:4px">Diferencia</div>
            <div style="font-size:16px;font-weight:700;color:${difColor}">${fmtQ(Math.abs(diferencia))}</div>
            <div style="font-size:10px;color:${difColor};margin-top:2px;font-weight:600">${difLabel}</div>
          </div>
        </div>
      </div>`;

    // ── Proyección cuotas por año ────────────────────────────────
    const aniosKeys = Object.keys(porAnio).sort();
    const anioRows = aniosKeys.map(yr => {
      const d = porAnio[yr];
      return `<tr>
        <td style="padding:6px 12px;font-weight:600;color:var(--dorado)">${yr}</td>
        <td style="padding:6px 12px;text-align:center;font-size:11px;color:var(--muted)">${d.n}</td>
        <td style="padding:6px 12px;text-align:right">${fmtQ(d.capital)}</td>
        <td style="padding:6px 12px;text-align:right;color:#e0a030">${fmtQ(d.interes)}</td>
        <td style="padding:6px 12px;text-align:right;font-weight:700">${fmtQ(d.cuota)}</td>
      </tr>`;
    }).join('');

    const totalPend_cap = Object.values(porAnio).reduce((s,d)=>s+d.capital,0);
    const totalPend_int = Object.values(porAnio).reduce((s,d)=>s+d.interes,0);
    const totalPend_cuo = Object.values(porAnio).reduce((s,d)=>s+d.cuota,0);

    const proyeccionHtml = aniosKeys.length ? `
      <div style="margin-bottom:14px">
        <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px">
          Proyección de pagos pendientes — por año
        </div>
        <table style="width:100%;font-size:12px;border-collapse:collapse">
          <thead>
            <tr style="background:var(--bg-section)">
              <th style="padding:7px 12px;text-align:left;font-size:11px;color:var(--muted);font-weight:700">Año</th>
              <th style="padding:7px 12px;text-align:center;font-size:11px;color:var(--muted);font-weight:700">Cuotas</th>
              <th style="padding:7px 12px;text-align:right;font-size:11px;color:var(--muted);font-weight:700">Capital</th>
              <th style="padding:7px 12px;text-align:right;font-size:11px;color:var(--muted);font-weight:700">Intereses</th>
              <th style="padding:7px 12px;text-align:right;font-size:11px;color:var(--muted);font-weight:700">Total cuota</th>
            </tr>
          </thead>
          <tbody>${anioRows}</tbody>
          <tfoot>
            <tr style="background:var(--bg-section);font-weight:700;border-top:2px solid var(--border)">
              <td style="padding:7px 12px">TOTAL PENDIENTE</td>
              <td style="padding:7px 12px;text-align:center">${cuotasPend.length}</td>
              <td style="padding:7px 12px;text-align:right;color:#e05050">${fmtQ(totalPend_cap)}</td>
              <td style="padding:7px 12px;text-align:right;color:#e0a030">${fmtQ(totalPend_int)}</td>
              <td style="padding:7px 12px;text-align:right;color:var(--dorado)">${fmtQ(totalPend_cuo)}</td>
            </tr>
          </tfoot>
        </table>
      </div>` : '';

    // ── Próximas 6 cuotas detalle ────────────────────────────────
    const proximas = cuotasPend.slice(0, 6);
    const proximasRows = proximas.map(c => {
      const venc = c.es_vencido;
      const label = venc ? `<span style="background:rgba(224,80,80,.2);color:#e05050;padding:2px 6px;border-radius:3px;font-size:9px;font-weight:700">VENC. ${c.cuotas_acumuladas||1} cuota(s)</span>` : '';
      return `<tr style="${venc?'background:rgba(224,80,80,.06)':''}">
        <td style="padding:5px 10px;font-size:11px" class="${venc?'text-red':''}"><strong>${c.op==='VENCIDO'?'Vencido':c.op}</strong></td>
        <td style="padding:5px 10px;font-size:11px">${c.fecha} ${label}</td>
        <td style="padding:5px 10px;font-size:11px;text-align:right">${fmtQ(c.saldo_capital||0)}</td>
        <td style="padding:5px 10px;font-size:11px;text-align:right">${fmtQ(c.capital)}</td>
        <td style="padding:5px 10px;font-size:11px;text-align:right;color:#e0a030">${fmtQ(c.interes)}</td>
        <td style="padding:5px 10px;font-size:11px;text-align:right;font-weight:700">${fmtQ(c.cuota)}</td>
      </tr>`;
    }).join('');

    const detalleHtml = proximas.length ? `
      <div>
        <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px">
          Detalle próximas cuotas
        </div>
        <table style="width:100%;font-size:11px;border-collapse:collapse">
          <thead>
            <tr style="background:var(--bg-section)">
              <th style="padding:6px 10px;text-align:left;font-size:10px;color:var(--muted)">Op.</th>
              <th style="padding:6px 10px;text-align:left;font-size:10px;color:var(--muted)">Fecha pago</th>
              <th style="padding:6px 10px;text-align:right;font-size:10px;color:var(--muted)">Saldo capital</th>
              <th style="padding:6px 10px;text-align:right;font-size:10px;color:var(--muted)">Capital</th>
              <th style="padding:6px 10px;text-align:right;font-size:10px;color:var(--muted)">Intereses</th>
              <th style="padding:6px 10px;text-align:right;font-size:10px;color:var(--muted)">Cuota total</th>
            </tr>
          </thead>
          <tbody>${proximasRows}</tbody>
        </table>
        ${cuotasPend.length > 6 ? `<div style="font-size:10px;color:var(--muted);text-align:center;padding:6px">+ ${cuotasPend.length-6} cuotas adicionales hasta ${cuotasPend[cuotasPend.length-1]?.fecha||'—'}</div>` : ''}
      </div>` : '';

    // ── Intercompany ──────────────────────────────────────────────
    const icHtml = icSaldo > 0 ? `
      <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 18px;margin-top:14px">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div>
            <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px">Saldo Intercompany</div>
            <div style="font-size:20px;font-weight:800;color:${icTipo==='por pagar'?'#e05050':'#2c7a4e'}">${fmtQ(icSaldo)}</div>
            <div style="font-size:11px;color:var(--muted);margin-top:2px">${icTipo === 'por pagar' ? 'Obligación pendiente con casa matriz' : 'Saldo a favor de la sociedad'}</div>
          </div>
          ${ic.dist_anual ? `<div style="text-align:right">
            <div style="font-size:10px;color:var(--muted);margin-bottom:2px">Distribución anual</div>
            <div style="font-size:16px;font-weight:700;color:var(--dorado)">${fmtQ(ic.dist_anual)}</div>
            <div style="font-size:10px;color:var(--muted)">sobre ${ic.anos_distribucion} años</div>
          </div>` : ''}
        </div>
      </div>` : '';

    container.innerHTML = kpiHtml + progressHtml + cuadreHtml + proyeccionHtml + detalleHtml + icHtml;

  } catch(e) {
    container.innerHTML = `<div style="color:var(--red);padding:20px;text-align:center">Error cargando egresos financieros: ${e.message}</div>`;
  }
}
