"""
proyecciones.py — Router FastAPI v3 — Proyecciones al Cierre
Módulos: Ingresos, Egresos Op (PPTO vs Ejecutado), Financieros (Préstamos+Intercompany), Tierra/Dividendos
"""

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import date, datetime
import json
import io

from app.database import get_db
from app.core.security import get_current_user

router = APIRouter(prefix="/api/proyecciones", tags=["proyecciones"])

# ─────────────────────────────────────────────
# DATOS ESTÁTICOS DESDE EXCEL (embebidos en código)
# ─────────────────────────────────────────────

# Presupuesto por sociedad (PPTO. PROYECTOS)
PPTO_PROYECTOS = {
    "Capipos":            {"urb": 2960886.97,  "adm": 5486340.625, "total": 8447227.595},
    "Ottavia":            {"urb": 32565496.94, "adm": 4435908.82,  "total": 37001405.76},
    "Vilet":              {"urb": 7776548.47,  "adm": 2529479.51,  "total": 10306027.98},
    "Tezzoli":            {"urb": 38712581.05, "adm": 5353552.61,  "total": 44066133.66},
    "Garbatella":         {"urb": 12140219.0,  "adm": 5234375.0,   "total": 17374594.0},
    "Urbiva":             {"urb": 18025593.4,  "adm": 4316677.41,  "total": 22342270.81},
    "Utilica":            {"urb": 15651579.86, "adm": 2740817.51,  "total": 18392397.37},
    "Corcolle":           {"urb": 2940898.26,  "adm": 681390.50,   "total": 3622288.76},
    "Leofreni":           {"urb": 4219549.67,  "adm": 977647.24,   "total": 5197196.91},
    "Gibraleon":          {"urb": 1150786.27,  "adm": 266631.07,   "total": 1417417.34},
    "Talocci":            {"urb": 3963819.39,  "adm": 918395.89,   "total": 4882215.28},
    "Eficiencia Urbana":  {"urb": 43682514.69, "adm": 12677589.09, "total": 56360103.77},
    "Ovest":              {"urb": 17956528.71, "adm": 2720307.85,  "total": 20676836.56},
    "Rossio":             {"urb": 14989329.93, "adm": 4209161.28,  "total": 19198491.21},
    "Servicios Generales":{"urb": 14416623.0,  "adm": 2836181.20,  "total": 17252804.20},
    "Frugalex":           {"urb": 8710467.37,  "adm": 3320576.61,  "total": 12031043.98},
}

# Mapeo empresa BD → clave PPTO
EMPRESA_TO_PPTO = {
    "Capipos, S.A.":               "Capipos",
    "Ottavia, S.A.":               "Ottavia",
    "Vilet, S.A.":                 "Vilet",
    "Tezzoli, S.A.":               "Tezzoli",
    "Garbatella, S.A.":            "Garbatella",
    "Urviba 2, S.A.":              "Urbiva",
    "Utilica, S.A.":               "Utilica",
    "Corcolle, S.A.":              "Corcolle",
    "Leofreni, S.A.":              "Leofreni",
    "Gibraleon, S.A.":             "Gibraleon",
    "Talocci, S.A.":               "Talocci",
    "Eficiencia Urbana, S. A.":    "Eficiencia Urbana",
    "Ovest, S.A.":                 "Ovest",
    "Rossio, S.A.":                "Rossio",
    "Servicios Generales CCC, S.A.":"Servicios Generales",
    "Frugalex, S.A.":              "Frugalex",
}

# Tablas de amortización de préstamos bancarios (desde PRESTAMOS BANCARIOS)
PRESTAMOS_AMORT = {
    "Eficiencia Urbana, S. A.": {
        "banco": "Banrural", "no_credito": "7991000667",
        "monto_original": 14000000, "tasa": 0.09,
        "cuotas": [
          {"op":1,"fecha":"2025-10-04","saldo_capital":14000000.0,"capital":0.0,"interes":57994.52,"cuota":57994.52},
          {"op":2,"fecha":"2025-11-04","saldo_capital":14000000.0,"capital":0.0,"interes":74909.59,"cuota":74909.59},
          {"op":3,"fecha":"2025-12-04","saldo_capital":14000000.0,"capital":72413.79,"interes":102526.03,"cuota":174939.82},
          {"op":4,"fecha":"2026-01-05","saldo_capital":13927586.21,"capital":72413.79,"interes":109154.66,"cuota":181568.45},
          {"op":5,"fecha":"2026-02-04","saldo_capital":13855172.42,"capital":72413.79,"interes":101916.39,"cuota":174330.18},
          {"op":6,"fecha":"2026-03-04","saldo_capital":13782758.63,"capital":72413.79,"interes":94123.33,"cuota":166537.12},
          {"op":7,"fecha":"2026-04-06","saldo_capital":13710344.84,"capital":132413.79,"interes":110887.48,"cuota":243301.27},
          {"op":8,"fecha":"2026-05-04","saldo_capital":13577931.05,"capital":132413.79,"interes":93743.52,"cuota":226157.31},
          {"op":9,"fecha":"2026-06-04","saldo_capital":13445517.26,"capital":132413.79,"interes":102775.32,"cuota":235189.11},
          {"op":10,"fecha":"2026-07-04","saldo_capital":13313103.47,"capital":132413.79,"interes":98480.49,"cuota":230894.28},
          {"op":11,"fecha":"2026-08-04","saldo_capital":13180689.68,"capital":132413.79,"interes":100751.03,"cuota":233164.82},
          {"op":12,"fecha":"2026-09-04","saldo_capital":13048275.89,"capital":132413.79,"interes":99738.88,"cuota":232152.67},
          {"op":13,"fecha":"2026-10-05","saldo_capital":12915862.1,"capital":132413.79,"interes":98726.73,"cuota":231140.52},
          {"op":14,"fecha":"2026-11-04","saldo_capital":12783448.31,"capital":132413.79,"interes":94562.49,"cuota":226976.28},
          {"op":15,"fecha":"2026-12-04","saldo_capital":12651034.52,"capital":132413.79,"interes":93583.0,"cuota":225996.79},
          {"op":16,"fecha":"2027-01-04","saldo_capital":12518620.73,"capital":132413.79,"interes":95690.28,"cuota":228104.07},
          {"op":17,"fecha":"2027-02-04","saldo_capital":12386206.94,"capital":132413.79,"interes":94678.13,"cuota":227091.92},
          {"op":18,"fecha":"2027-03-04","saldo_capital":12253793.15,"capital":132413.79,"interes":84601.53,"cuota":217015.32},
          {"op":19,"fecha":"2027-04-05","saldo_capital":12121379.36,"capital":132413.79,"interes":95642.66,"cuota":228056.45},
          {"op":20,"fecha":"2027-05-04","saldo_capital":11988965.57,"capital":132413.79,"interes":85729.32,"cuota":218143.11},
          {"op":21,"fecha":"2027-06-04","saldo_capital":11856551.78,"capital":132413.79,"interes":90629.53,"cuota":223043.32},
          {"op":22,"fecha":"2027-07-05","saldo_capital":11724137.99,"capital":132413.79,"interes":89617.38,"cuota":222031.17},
          {"op":23,"fecha":"2027-08-04","saldo_capital":11591724.2,"capital":132413.79,"interes":85747.0,"cuota":218160.79},
          {"op":24,"fecha":"2027-09-04","saldo_capital":11459310.41,"capital":132413.79,"interes":87593.09,"cuota":220006.88},
          {"op":25,"fecha":"2027-10-04","saldo_capital":11326896.62,"capital":132413.79,"interes":83788.0,"cuota":216201.79},
          {"op":26,"fecha":"2027-11-04","saldo_capital":11194482.83,"capital":132413.79,"interes":85568.79,"cuota":217982.58},
          {"op":27,"fecha":"2027-12-04","saldo_capital":11062069.04,"capital":132413.79,"interes":81829.0,"cuota":214242.79},
          {"op":28,"fecha":"2028-01-04","saldo_capital":10929655.25,"capital":132413.79,"interes":83544.49,"cuota":215958.28},
          {"op":29,"fecha":"2028-02-04","saldo_capital":10797241.46,"capital":132413.79,"interes":82532.34,"cuota":214946.13},
          {"op":30,"fecha":"2028-03-04","saldo_capital":10664827.67,"capital":132413.79,"interes":76260.82,"cuota":208674.61},
          {"op":31,"fecha":"2028-04-04","saldo_capital":10532413.88,"capital":132413.79,"interes":80508.04,"cuota":212921.83},
          {"op":32,"fecha":"2028-05-04","saldo_capital":10400000.09,"capital":132413.79,"interes":76931.51,"cuota":209345.30},
          {"op":33,"fecha":"2028-06-05","saldo_capital":10267586.3,"capital":132413.79,"interes":81015.48,"cuota":213429.27},
          {"op":34,"fecha":"2028-07-04","saldo_capital":10135172.51,"capital":132413.79,"interes":72473.43,"cuota":204887.22},
          {"op":35,"fecha":"2028-08-04","saldo_capital":10002758.72,"capital":132413.79,"interes":76459.44,"cuota":208873.23},
          {"op":36,"fecha":"2028-09-04","saldo_capital":9870344.93,"capital":132413.79,"interes":75447.29,"cuota":207861.08},
          {"op":37,"fecha":"2028-10-04","saldo_capital":9737931.14,"capital":132413.79,"interes":72034.01,"cuota":204447.80},
          {"op":38,"fecha":"2028-11-04","saldo_capital":9605517.35,"capital":132413.79,"interes":73423.0,"cuota":205836.79},
          {"op":39,"fecha":"2028-12-04","saldo_capital":9473103.56,"capital":132413.79,"interes":70075.01,"cuota":202488.80},
          {"op":40,"fecha":"2029-01-04","saldo_capital":9340689.77,"capital":132413.79,"interes":71398.7,"cuota":203812.49},
          {"op":41,"fecha":"2029-02-05","saldo_capital":9208275.98,"capital":132413.79,"interes":72657.08,"cuota":205070.87},
          {"op":42,"fecha":"2029-03-05","saldo_capital":9075862.19,"capital":132413.79,"interes":62660.75,"cuota":195074.54},
          {"op":43,"fecha":"2029-04-04","saldo_capital":8943448.4,"capital":132413.79,"interes":66157.02,"cuota":198570.81},
          {"op":44,"fecha":"2029-05-04","saldo_capital":8811034.61,"capital":132413.79,"interes":65177.52,"cuota":197591.31},
          {"op":45,"fecha":"2029-06-04","saldo_capital":8678620.82,"capital":132413.79,"interes":66337.95,"cuota":198751.74},
          {"op":46,"fecha":"2029-07-04","saldo_capital":8546207.03,"capital":132413.79,"interes":63218.52,"cuota":195632.31},
          {"op":47,"fecha":"2029-08-04","saldo_capital":8413793.24,"capital":132413.79,"interes":64313.65,"cuota":196727.44},
          {"op":48,"fecha":"2029-09-04","saldo_capital":8281379.45,"capital":132413.79,"interes":63301.5,"cuota":195715.29},
          {"op":49,"fecha":"2029-10-04","saldo_capital":8148965.66,"capital":132413.79,"interes":60280.02,"cuota":192693.81},
          {"op":50,"fecha":"2029-11-04","saldo_capital":8016551.87,"capital":132413.79,"interes":61274.65,"cuota":193688.44},
          {"op":51,"fecha":"2029-12-04","saldo_capital":7884138.08,"capital":132413.79,"interes":58318.16,"cuota":190731.95},
          {"op":52,"fecha":"2030-01-06","saldo_capital":7751724.29,"capital":132413.79,"interes":60873.41,"cuota":193287.20},
          {"op":53,"fecha":"2030-02-04","saldo_capital":7619310.5,"capital":132413.79,"interes":58251.73,"cuota":190665.52},
          {"op":54,"fecha":"2030-03-04","saldo_capital":7486896.71,"capital":132413.79,"interes":51727.45,"cuota":184141.24},
          {"op":55,"fecha":"2030-04-04","saldo_capital":7354482.92,"capital":132413.79,"interes":56222.95,"cuota":188636.74},
          {"op":56,"fecha":"2030-05-04","saldo_capital":7222069.13,"capital":132413.79,"interes":53439.51,"cuota":185853.30},
          {"op":57,"fecha":"2030-06-04","saldo_capital":7089655.34,"capital":132413.79,"interes":54192.72,"cuota":186606.51},
          {"op":58,"fecha":"2030-07-04","saldo_capital":6957241.55,"capital":132413.79,"interes":51474.78,"cuota":183888.57},
          {"op":59,"fecha":"2030-08-04","saldo_capital":6824827.76,"capital":132413.79,"interes":52155.95,"cuota":184569.74},
          {"op":60,"fecha":"2030-09-04","saldo_capital":6692413.97,"capital":6692413.97,"interes":49505.53,"cuota":6741919.5},
        ]
    },
    "Utilica, S.A.": {
        "banco": "Banrural", "no_credito": "7991000653",
        "monto_original": 5600000, "tasa": 0.09,
        "cuotas": [
          {"op":1,"fecha":"2025-10-06","saldo_capital":5600000.0,"capital":0.0,"interes":53852.05,"cuota":53852.05},
          {"op":2,"fecha":"2025-11-05","saldo_capital":5600000.0,"capital":0.0,"interes":41424.66,"cuota":41424.66},
          {"op":3,"fecha":"2025-12-05","saldo_capital":5600000.0,"capital":0.0,"interes":41424.66,"cuota":41424.66},
          {"op":4,"fecha":"2026-01-05","saldo_capital":5600000.0,"capital":93333.33,"interes":43517.81,"cuota":136851.14},
          {"op":5,"fecha":"2026-02-05","saldo_capital":5506666.67,"capital":93333.33,"interes":40758.47,"cuota":134091.80},
          {"op":6,"fecha":"2026-03-05","saldo_capital":5413333.34,"capital":93333.33,"interes":37063.28,"cuota":130396.61},
          {"op":7,"fecha":"2026-04-05","saldo_capital":5320000.01,"capital":93333.33,"interes":40661.64,"cuota":133994.97},
          {"op":8,"fecha":"2026-05-05","saldo_capital":5226666.68,"capital":93333.33,"interes":38683.01,"cuota":132016.34},
          {"op":9,"fecha":"2026-06-05","saldo_capital":5133333.35,"capital":93333.33,"interes":39239.73,"cuota":132573.06},
          {"op":10,"fecha":"2026-07-05","saldo_capital":5040000.02,"capital":93333.33,"interes":37295.89,"cuota":130629.22},
          {"op":11,"fecha":"2026-08-05","saldo_capital":4946666.69,"capital":93333.33,"interes":37817.81,"cuota":131151.14},
          {"op":12,"fecha":"2026-09-05","saldo_capital":4853333.36,"capital":93333.33,"interes":37092.47,"cuota":130425.80},
          {"op":13,"fecha":"2026-10-05","saldo_capital":4760000.03,"capital":93333.33,"interes":35314.52,"cuota":128647.85},
          {"op":14,"fecha":"2026-11-05","saldo_capital":4666666.70,"capital":93333.33,"interes":34545.21,"cuota":127878.54},
          {"op":15,"fecha":"2026-12-05","saldo_capital":4573333.37,"capital":93333.33,"interes":33819.18,"cuota":127152.51},
          {"op":16,"fecha":"2027-01-05","saldo_capital":4480000.04,"capital":93333.33,"interes":34238.36,"cuota":127571.69},
          {"op":17,"fecha":"2027-02-05","saldo_capital":4386666.71,"capital":93333.33,"interes":33513.01,"cuota":126846.34},
          {"op":18,"fecha":"2027-03-05","saldo_capital":4293333.38,"capital":93333.33,"interes":29634.25,"cuota":122967.58},
          {"op":19,"fecha":"2027-04-05","saldo_capital":4200000.05,"capital":93333.33,"interes":32100.0,"cuota":125433.33},
          {"op":20,"fecha":"2027-05-05","saldo_capital":4106666.72,"capital":93333.33,"interes":30393.15,"cuota":123726.48},
          {"op":21,"fecha":"2027-06-05","saldo_capital":4013333.39,"capital":93333.33,"interes":30660.27,"cuota":123993.60},
          {"op":22,"fecha":"2027-07-05","saldo_capital":3920000.06,"capital":93333.33,"interes":29952.33,"cuota":123285.66},
          {"op":23,"fecha":"2027-08-05","saldo_capital":3826666.73,"capital":93333.33,"interes":29260.27,"cuota":122593.60},
          {"op":24,"fecha":"2027-09-05","saldo_capital":3733333.40,"capital":93333.33,"interes":28511.51,"cuota":121844.84},
          {"op":25,"fecha":"2027-10-05","saldo_capital":3640000.07,"capital":93333.33,"interes":26958.90,"cuota":120292.23},
          {"op":26,"fecha":"2027-11-05","saldo_capital":3546666.74,"capital":93333.33,"interes":26219.18,"cuota":119552.51},
          {"op":27,"fecha":"2027-12-05","saldo_capital":3453333.41,"capital":93333.33,"interes":25616.44,"cuota":118949.77},
          {"op":28,"fecha":"2028-01-05","saldo_capital":3360000.08,"capital":93333.33,"interes":25679.45,"cuota":119012.78},
          {"op":29,"fecha":"2028-02-05","saldo_capital":3266666.75,"capital":93333.33,"interes":24972.60,"cuota":118305.93},
          {"op":30,"fecha":"2028-03-05","saldo_capital":3173333.42,"capital":93333.33,"interes":21923.29,"cuota":115256.62},
          {"op":31,"fecha":"2028-04-05","saldo_capital":3080000.09,"capital":93333.33,"interes":23527.40,"cuota":116860.73},
          {"op":32,"fecha":"2028-05-05","saldo_capital":2986666.76,"capital":93333.33,"interes":22116.44,"cuota":115449.77},
          {"op":33,"fecha":"2028-06-05","saldo_capital":2893333.43,"capital":93333.33,"interes":22118.63,"cuota":115451.96},
          {"op":34,"fecha":"2028-07-05","saldo_capital":2800000.10,"capital":93333.33,"interes":20720.55,"cuota":114053.88},
          {"op":35,"fecha":"2028-08-05","saldo_capital":2706666.77,"capital":93333.33,"interes":20684.93,"cuota":114018.26},
          {"op":36,"fecha":"2028-09-05","saldo_capital":2613333.44,"capital":93333.33,"interes":19979.45,"cuota":113312.78},
          {"op":37,"fecha":"2028-10-05","saldo_capital":2520000.11,"capital":93333.33,"interes":18655.07,"cuota":111988.40},
          {"op":38,"fecha":"2028-11-05","saldo_capital":2426666.78,"capital":93333.33,"interes":17945.21,"cuota":111278.54},
          {"op":39,"fecha":"2028-12-05","saldo_capital":2333333.45,"capital":93333.33,"interes":17268.49,"cuota":110601.82},
          {"op":40,"fecha":"2029-01-05","saldo_capital":2240000.12,"capital":93333.33,"interes":17123.29,"cuota":110456.62},
          {"op":41,"fecha":"2029-02-05","saldo_capital":2146666.79,"capital":93333.33,"interes":16431.51,"cuota":109764.84},
          {"op":42,"fecha":"2029-03-05","saldo_capital":2053333.46,"capital":93333.33,"interes":14175.34,"cuota":107508.67},
          {"op":43,"fecha":"2029-04-05","saldo_capital":1960000.13,"capital":93333.33,"interes":14980.27,"cuota":108313.60},
          {"op":44,"fecha":"2029-05-05","saldo_capital":1866666.80,"capital":93333.33,"interes":13819.18,"cuota":107152.51},
          {"op":45,"fecha":"2029-06-05","saldo_capital":1773333.47,"capital":93333.33,"interes":13551.78,"cuota":106885.11},
          {"op":46,"fecha":"2029-07-05","saldo_capital":1680000.14,"capital":93333.33,"interes":12427.40,"cuota":105760.73},
          {"op":47,"fecha":"2029-08-05","saldo_capital":1586666.81,"capital":93333.33,"interes":12127.40,"cuota":105460.73},
          {"op":48,"fecha":"2029-09-05","saldo_capital":1493333.48,"capital":93333.33,"interes":11416.44,"cuota":104749.77},
          {"op":49,"fecha":"2029-10-05","saldo_capital":1400000.15,"capital":93333.33,"interes":10367.12,"cuota":103700.45},
          {"op":50,"fecha":"2029-11-05","saldo_capital":1306666.82,"capital":93333.33,"interes":9672.88,"cuota":103006.21},
          {"op":51,"fecha":"2029-12-05","saldo_capital":1213333.49,"capital":93333.33,"interes":8979.45,"cuota":102312.78},
          {"op":52,"fecha":"2030-01-05","saldo_capital":1120000.16,"capital":93333.33,"interes":8562.74,"cuota":101896.07},
          {"op":53,"fecha":"2030-02-05","saldo_capital":1026666.83,"capital":93333.33,"interes":7843.15,"cuota":101176.48},
          {"op":54,"fecha":"2030-03-05","saldo_capital":933333.50,"capital":93333.33,"interes":6438.36,"cuota":99771.69},
          {"op":55,"fecha":"2030-04-05","saldo_capital":840000.17,"capital":93333.33,"interes":6424.93,"cuota":99758.26},
          {"op":56,"fecha":"2030-05-05","saldo_capital":746666.84,"capital":93333.33,"interes":5525.21,"cuota":98858.54},
          {"op":57,"fecha":"2030-06-05","saldo_capital":653333.51,"capital":93333.33,"interes":4993.15,"cuota":98326.48},
          {"op":58,"fecha":"2030-07-05","saldo_capital":560000.18,"capital":93333.33,"interes":4143.84,"cuota":97477.17},
          {"op":59,"fecha":"2030-08-05","saldo_capital":466666.85,"capital":93333.33,"interes":3567.12,"cuota":96900.45},
          {"op":60,"fecha":"2030-09-05","saldo_capital":156666.63,"capital":156666.63,"interes":1197.53,"cuota":157864.16},
        ]
    },
    "Servicios Generales CCC, S.A.": {
        "banco": "Banrural", "no_credito": "7951016725",
        "monto_original": 3000000, "tasa": 0.09,
        "cuotas": []  # Se cargan dinámicamente desde la BD - préstamo casi liquidado
    },
    "Capipos, S.A.": {
        "banco": "G&T Continental", "no_credito": "10-060179247",
        "monto_original": 11700000, "tasa": 0.0925,
        "cuotas": [
          {"op":1,"fecha":"2024-05-05","saldo_capital":11700000.0,"capital":0.0,"interes":14825.34,"cuota":14825.34},
          {"op":2,"fecha":"2024-06-05","saldo_capital":11700000.0,"capital":0.0,"interes":91917.12,"cuota":91917.12},
          {"op":3,"fecha":"2024-07-05","saldo_capital":11700000.0,"capital":0.0,"interes":88952.05,"cuota":88952.05},
          {"op":4,"fecha":"2024-08-05","saldo_capital":11700000.0,"capital":0.0,"interes":91917.12,"cuota":91917.12},
          {"op":5,"fecha":"2024-09-05","saldo_capital":11700000.0,"capital":0.0,"interes":88952.05,"cuota":88952.05},
          {"op":6,"fecha":"2024-10-05","saldo_capital":11700000.0,"capital":0.0,"interes":91917.12,"cuota":91917.12},
          {"op":7,"fecha":"2024-11-05","saldo_capital":11700000.0,"capital":0.0,"interes":88952.05,"cuota":88952.05},
          {"op":8,"fecha":"2024-12-05","saldo_capital":11700000.0,"capital":0.0,"interes":91917.12,"cuota":91917.12},
          {"op":9,"fecha":"2025-01-05","saldo_capital":11700000.0,"capital":0.0,"interes":88952.05,"cuota":88952.05},
          {"op":10,"fecha":"2025-02-05","saldo_capital":11700000.0,"capital":0.0,"interes":80304.66,"cuota":80304.66},
          {"op":11,"fecha":"2025-03-05","saldo_capital":11700000.0,"capital":0.0,"interes":88952.05,"cuota":88952.05},
          {"op":12,"fecha":"2025-04-05","saldo_capital":11700000.0,"capital":0.0,"interes":88952.05,"cuota":88952.05},
          {"op":13,"fecha":"2025-05-05","saldo_capital":11700000.0,"capital":292500.0,"interes":91917.12,"cuota":384417.12},
          {"op":14,"fecha":"2025-06-05","saldo_capital":11407500.0,"capital":292500.0,"interes":89626.44,"cuota":382126.44},
          {"op":15,"fecha":"2025-07-05","saldo_capital":11115000.0,"capital":292500.0,"interes":87332.88,"cuota":379832.88},
          {"op":16,"fecha":"2025-08-05","saldo_capital":10822500.0,"capital":292500.0,"interes":85043.84,"cuota":377543.84},
          {"op":17,"fecha":"2025-09-05","saldo_capital":10530000.0,"capital":292500.0,"interes":80475.41,"cuota":372975.41},
          {"op":18,"fecha":"2025-10-05","saldo_capital":10237500.0,"capital":292500.0,"interes":80456.51,"cuota":372956.51},
          {"op":19,"fecha":"2025-11-05","saldo_capital":9945000.0,"capital":292500.0,"interes":78166.03,"cuota":370666.03},
          {"op":20,"fecha":"2025-12-05","saldo_capital":9652500.0,"capital":292500.0,"interes":75876.99,"cuota":368376.99},
          {"op":21,"fecha":"2026-01-05","saldo_capital":9360000.0,"capital":292500.0,"interes":71308.77,"cuota":363808.77},
          {"op":22,"fecha":"2026-02-05","saldo_capital":9067500.0,"capital":292500.0,"interes":71298.97,"cuota":363798.97},
          {"op":23,"fecha":"2026-03-05","saldo_capital":8775000.0,"capital":292500.0,"interes":66822.95,"cuota":359322.95},
          {"op":24,"fecha":"2026-04-05","saldo_capital":8482500.0,"capital":292500.0,"interes":66730.48,"cuota":359230.48},
          {"op":25,"fecha":"2026-05-05","saldo_capital":8190000.0,"capital":292500.0,"interes":64335.62,"cuota":356835.62},
          {"op":26,"fecha":"2026-06-05","saldo_capital":7897500.0,"capital":292500.0,"interes":62069.18,"cuota":354569.18},
          {"op":27,"fecha":"2026-07-05","saldo_capital":7605000.0,"capital":292500.0,"interes":59778.08,"cuota":352278.08},
          {"op":28,"fecha":"2026-08-05","saldo_capital":7312500.0,"capital":292500.0,"interes":57487.36,"cuota":349987.36},
          {"op":29,"fecha":"2026-09-05","saldo_capital":7020000.0,"capital":292500.0,"interes":55169.59,"cuota":347669.59},
          {"op":30,"fecha":"2026-10-05","saldo_capital":6727500.0,"capital":292500.0,"interes":52878.49,"cuota":345378.49},
          {"op":31,"fecha":"2026-11-05","saldo_capital":6435000.0,"capital":292500.0,"interes":50561.10,"cuota":343061.10},
          {"op":32,"fecha":"2026-12-05","saldo_capital":6142500.0,"capital":292500.0,"interes":48270.0,"cuota":340770.0},
          {"op":33,"fecha":"2027-01-05","saldo_capital":5850000.0,"capital":292500.0,"interes":44721.92,"cuota":337221.92},
          {"op":34,"fecha":"2027-02-05","saldo_capital":5557500.0,"capital":292500.0,"interes":43691.51,"cuota":336191.51},
          {"op":35,"fecha":"2027-03-05","saldo_capital":5265000.0,"capital":292500.0,"interes":40088.22,"cuota":332588.22},
          {"op":36,"fecha":"2027-04-05","saldo_capital":4972500.0,"capital":292500.0,"interes":39070.27,"cuota":331570.27},
          {"op":37,"fecha":"2027-05-05","saldo_capital":4680000.0,"capital":292500.0,"interes":36780.0,"cuota":329280.0},
          {"op":38,"fecha":"2027-06-05","saldo_capital":4387500.0,"capital":292500.0,"interes":34489.73,"cuota":326989.73},
          {"op":39,"fecha":"2027-07-05","saldo_capital":4095000.0,"capital":292500.0,"interes":32172.33,"cuota":324672.33},
          {"op":40,"fecha":"2027-08-05","saldo_capital":3802500.0,"capital":292500.0,"interes":29881.23,"cuota":322381.23},
          {"op":41,"fecha":"2027-09-05","saldo_capital":3510000.0,"capital":292500.0,"interes":27563.84,"cuota":320063.84},
          {"op":42,"fecha":"2027-10-05","saldo_capital":3217500.0,"capital":292500.0,"interes":25272.74,"cuota":317772.74},
          {"op":43,"fecha":"2027-11-05","saldo_capital":2925000.0,"capital":292500.0,"interes":22955.34,"cuota":315455.34},
          {"op":44,"fecha":"2027-12-05","saldo_capital":2632500.0,"capital":292500.0,"interes":20665.07,"cuota":313165.07},
          {"op":45,"fecha":"2028-01-05","saldo_capital":2340000.0,"capital":292500.0,"interes":17880.0,"cuota":310380.0},
          {"op":46,"fecha":"2028-02-05","saldo_capital":2047500.0,"capital":292500.0,"interes":16087.89,"cuota":308587.89},
          {"op":47,"fecha":"2028-03-05","saldo_capital":1755000.0,"capital":292500.0,"interes":13384.11,"cuota":305884.11},
          {"op":48,"fecha":"2028-04-05","saldo_capital":1462500.0,"capital":292500.0,"interes":11490.41,"cuota":303990.41},
          {"op":49,"fecha":"2028-04-29","saldo_capital":0.0,"capital":11700000.0,"interes":71161.64,"cuota":11771161.64},
        ]
    },
}

# Plan de pagos tierra (resumido por sociedad, fecha, monto)
PLAN_TIERRA = [
    {"sociedad":"EFICIENCIA URBANA","concepto":"Desarrollo","pagos":{"2025-04-01":700000,"2025-05-01":111111.11,"2025-06-01":111111.11,"2025-07-01":111111.11,"2025-08-01":111111.11,"2025-09-01":111111.11,"2025-10-01":111111.11,"2025-11-01":111111.11,"2025-12-01":111111.11,"2026-01-01":111111.11,"2026-02-01":111111.11,"2026-03-01":111111.11,"2026-04-01":111111.11,"2026-05-01":111111.11,"2026-06-01":111111.11,"2026-07-01":111111.11,"2026-08-01":111111.11,"2026-09-01":111111.11,"2026-10-01":111111.11}},
    {"sociedad":"EFICIENCIA URBANA","concepto":"Pago de Tierra","pagos":{"2025-01-01":100000,"2025-02-01":100000,"2025-03-01":100000,"2025-04-01":100000,"2025-05-01":100000,"2025-06-01":100000,"2025-07-01":150000,"2025-08-01":150000,"2025-09-01":150000,"2025-10-01":150000,"2025-11-01":150000,"2025-12-01":150000,"2026-01-01":150000,"2026-02-01":150000,"2026-03-01":150000,"2026-04-01":150000,"2026-05-01":150000,"2026-06-01":150000,"2026-07-01":150000,"2026-08-01":150000,"2026-09-01":150000,"2026-10-01":150000,"2026-11-01":150000,"2026-12-01":150000,"2027-01-01":150000,"2027-02-01":150000,"2027-03-01":150000,"2027-04-01":150000,"2027-05-01":150000,"2027-06-01":150000,"2027-07-01":1725000}},
    {"sociedad":"UTILICA","concepto":"Pago de Tierra","pagos":{"2025-02-01":50000,"2025-03-01":700000,"2025-11-01":1222500,"2026-01-01":118541.66,"2026-02-01":118541.66,"2026-03-01":118541.66,"2026-04-01":118541.66,"2026-05-01":118541.66,"2026-06-01":118541.66,"2026-07-01":118541.66,"2026-08-01":118541.66,"2026-09-01":118541.66,"2026-10-01":118541.66,"2026-11-01":118541.66,"2026-12-01":118541.74}},
    {"sociedad":"TEZZOLI","concepto":"Pago de Tierra","pagos":{"2025-08-01":200000,"2025-09-01":200000,"2025-10-01":200000,"2025-11-01":200000,"2025-12-01":200000,"2026-01-01":200000,"2026-02-01":200000,"2026-03-01":200000,"2026-04-01":200000,"2026-05-01":200000,"2026-06-01":200000,"2026-07-01":200000,"2026-08-01":200000,"2026-09-01":200000,"2026-10-01":200000,"2026-11-01":200000,"2026-12-01":200000,"2027-01-01":200000,"2027-02-01":200000,"2027-03-01":200000,"2027-04-01":200000,"2027-05-01":200000,"2027-06-01":200000,"2027-07-01":200000,"2027-08-01":200000,"2027-09-01":200000,"2027-10-01":200000,"2027-11-01":200000,"2027-12-01":200000,"2028-01-01":200000,"2028-02-01":200000,"2028-03-01":200000,"2028-04-01":200000,"2028-05-01":200000,"2028-06-01":200000,"2028-07-01":200000,"2028-08-01":200000,"2028-09-01":200000,"2028-10-01":200000,"2028-11-01":200000,"2028-12-01":200000,"2029-01-01":200000,"2029-02-01":200000,"2029-03-01":200000,"2029-04-01":200000,"2029-05-01":200000,"2029-06-01":200000,"2029-07-01":1200000,"2031-01-01":366666.67,"2031-02-01":366666.67,"2031-03-01":366666.67,"2031-04-01":366666.67,"2031-05-01":366666.67,"2031-06-01":366666.67,"2031-07-01":366666.67,"2031-08-01":366666.67,"2031-09-01":366666.67,"2031-10-01":366666.67,"2031-11-01":366666.67,"2031-12-01":366666.67,"2032-01-01":366666.67,"2032-02-01":366666.67,"2032-03-01":366666.67,"2032-04-01":366666.67,"2032-05-01":366666.67,"2032-06-01":366666.67,"2032-07-01":366666.67,"2032-08-01":366666.67,"2032-09-01":366666.67,"2032-10-01":366666.67,"2032-11-01":366666.67,"2032-12-01":366666.67}},
    {"sociedad":"OTTAVIA","concepto":"Pago de Tierra","pagos":{"2025-07-01":250000,"2026-08-01":100000,"2026-09-01":100000,"2026-10-01":100000,"2026-11-01":100000,"2026-12-01":100000,"2027-01-01":100000,"2027-02-01":100000,"2027-03-01":100000,"2027-04-01":100000,"2027-05-01":100000,"2027-06-01":100000,"2027-07-01":100000,"2027-08-01":100000,"2027-09-01":100000,"2027-10-01":100000,"2027-11-01":100000,"2027-12-01":100000,"2028-01-01":100000,"2028-02-01":100000,"2028-03-01":100000,"2028-04-01":380851,"2028-05-01":380851,"2028-06-01":380851,"2028-07-01":380851,"2028-08-01":380851,"2028-09-01":380851,"2028-10-01":380851,"2028-11-01":380851,"2028-12-01":380851,"2029-01-01":380851,"2029-02-01":380851,"2029-03-01":380851,"2029-04-01":380851,"2029-05-01":380851,"2029-06-01":380851,"2029-07-01":380851,"2029-08-01":380851,"2029-09-01":380851,"2029-10-01":380851,"2029-11-01":380851,"2029-12-01":380851,"2030-01-01":380851,"2030-02-01":380851,"2030-03-01":380851,"2030-04-01":380851,"2030-05-01":380851,"2030-06-01":380851,"2030-07-01":380851,"2030-08-01":380851,"2030-09-01":380851,"2030-10-01":380851,"2030-11-01":380851,"2030-12-01":380851,"2031-01-01":380851,"2031-02-01":380851,"2031-03-01":380851,"2031-04-01":750003}},
    {"sociedad":"ROSSIO","concepto":"Pago de Tierra","pagos":{"2025-01-01":300000,"2025-04-01":200000,"2025-06-01":25000,"2025-07-01":25000,"2025-08-01":25000,"2025-09-01":25000,"2025-10-01":25000,"2025-11-01":25000,"2025-12-01":25000,"2026-01-01":25000,"2026-02-01":25000,"2026-03-01":25000,"2026-04-01":100000,"2026-05-01":100000,"2026-06-01":100000,"2026-07-01":100000,"2026-08-01":100000,"2026-09-01":100000,"2026-10-01":100000,"2026-11-01":100000,"2026-12-01":100000,"2027-01-01":100000,"2027-02-01":100000,"2027-03-01":100000,"2027-04-01":150000,"2027-05-01":150000,"2027-06-01":150000,"2027-07-01":150000,"2027-08-01":150000,"2027-09-01":150000,"2027-10-01":150000,"2027-11-01":150000,"2027-12-01":150000,"2028-01-01":150000,"2028-02-01":150000,"2028-03-01":150000}},
    {"sociedad":"OVEST","concepto":"Pago de Tierra","pagos":{"2025-08-01":65000,"2025-09-01":65000,"2025-10-01":65000,"2025-11-01":65000,"2025-12-01":65000,"2026-01-01":293450.79,"2026-02-01":293450.79,"2026-03-01":293450.79,"2026-04-01":293450.79,"2026-05-01":293450.79,"2026-06-01":293450.79,"2026-07-01":293450.79,"2026-08-01":293450.79,"2026-09-01":293450.79,"2026-10-01":293450.79,"2026-11-01":293450.79,"2026-12-01":293450.79,"2027-01-01":293450.79,"2027-02-01":293450.79,"2027-03-01":293450.79,"2027-04-01":293450.79,"2027-05-01":293450.79,"2027-06-01":293450.79,"2027-07-01":293450.79,"2027-08-01":293450.79,"2027-09-01":293450.79,"2027-10-01":293450.79,"2027-11-01":293450.79,"2027-12-01":293450.79,"2028-01-01":293450.79,"2028-02-01":293450.79,"2028-03-01":293450.79,"2028-04-01":293450.79,"2028-05-01":293450.79,"2028-06-01":293450.79,"2028-07-01":293450.79,"2028-08-01":293450.79,"2028-09-01":293450.79,"2028-10-01":293450.79,"2028-11-01":293450.79,"2028-12-01":262722.35}},
    {"sociedad":"VILET","concepto":"Pago de Tierra","pagos":{"2025-07-01":150000,"2026-03-01":301388.89,"2026-04-01":301388.89,"2026-05-01":301388.89,"2026-06-01":301388.89,"2026-07-01":301388.89,"2026-08-01":301388.89,"2026-09-01":301388.89,"2026-10-01":301388.89,"2026-11-01":301388.89,"2026-12-01":301388.89,"2027-01-01":301388.89,"2027-02-01":301388.89,"2027-03-01":301388.89,"2027-04-01":301388.89,"2027-05-01":301388.89,"2027-06-01":301388.89,"2027-07-01":301388.89,"2027-08-01":301388.89,"2027-09-01":301388.89,"2027-10-01":301388.89,"2027-11-01":301388.89,"2027-12-01":301388.89,"2028-01-01":301388.89,"2028-02-01":301388.89,"2028-03-01":301388.89,"2028-04-01":301388.89,"2028-05-01":301388.89,"2028-06-01":301388.89,"2028-07-01":301388.89,"2028-08-01":301388.89,"2028-09-01":301388.89,"2028-10-01":301388.89,"2028-11-01":301388.89,"2028-12-01":301388.89,"2029-01-01":301388.89,"2029-02-01":301388.89}},
    {"sociedad":"GARBATELLA","concepto":"Pago de Tierra","pagos":{"2025-11-01":2000000,"2026-01-01":1000000,"2026-02-01":2318750,"2026-03-01":318750,"2026-04-01":318750,"2026-05-01":318750,"2026-06-01":318750,"2026-07-01":318750,"2026-08-01":318750,"2026-09-01":318750,"2026-10-01":318750,"2026-11-01":318750,"2027-01-01":318750,"2027-02-01":318750,"2027-03-01":318750,"2027-04-01":318750,"2027-05-01":318750}},
]

# Plan dividendos/utilidades
PLAN_DIVIDENDOS = [
    {"sociedad":"EFICIENCIA URBANA","cuenta":"3 Holdings","pagos":{"2025-09-01":950000.02,"2025-11-01":47500.04,"2025-12-01":47500,"2026-01-01":100000.04,"2026-02-01":950000.05,"2026-03-01":47500,"2026-04-01":47500,"2026-05-01":95000,"2026-06-01":95000,"2026-07-01":118750,"2026-08-01":118750,"2026-09-01":118750,"2026-10-01":118750,"2026-11-01":660000}},
    {"sociedad":"EFICIENCIA URBANA","cuenta":"Marme Group","pagos":{"2025-09-01":950000.02,"2025-11-01":47500.04,"2025-12-01":47500,"2026-01-01":100000.04,"2026-02-01":950000.05,"2026-03-01":47500,"2026-04-01":47500,"2026-05-01":95000,"2026-06-01":95000,"2026-07-01":118750,"2026-08-01":118750,"2026-09-01":118750,"2026-10-01":118750,"2026-11-01":660000}},
    {"sociedad":"EFICIENCIA URBANA","cuenta":"Socio A JFS y JS","pagos":{"2028-12-01":250000,"2029-12-01":500000,"2030-07-01":2000000,"2030-12-01":700000,"2031-07-01":2500000,"2032-07-01":1250000}},
    {"sociedad":"CORCOLLE","cuenta":"Dividendos","pagos":{"2025-07-01":51756,"2026-08-01":12987,"2026-09-01":12987,"2026-10-01":12987,"2026-11-01":12987,"2026-12-01":12987,"2027-01-01":12987,"2027-02-01":12987,"2027-03-01":12987,"2027-04-01":12987,"2027-05-01":12987,"2027-06-01":12987,"2027-07-01":40240,"2027-08-01":40240,"2027-09-01":40240,"2027-10-01":40240,"2027-11-01":40240,"2027-12-01":40240,"2028-01-01":40240,"2028-02-01":40240,"2028-03-01":40240,"2028-04-01":40240,"2028-05-01":40240,"2028-06-01":40240,"2028-07-01":40240,"2028-08-01":40240,"2028-09-01":40240,"2028-10-01":40240,"2028-11-01":40240,"2028-12-01":40240,"2029-01-01":40240,"2029-02-01":40240,"2029-03-01":40240,"2029-04-01":40240,"2029-05-01":40240,"2029-06-01":40240,"2029-07-01":40240}},
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def calcular_tir(flujos, max_iter=200, tol=1e-6):
    """TIR con manejo de overflow."""
    if not flujos or all(f == 0 for f in flujos): return None
    if all(f >= 0 for f in flujos) or all(f <= 0 for f in flujos): return None
    try:
        tasa = 0.10
        for _ in range(max_iter):
            try:
                npv   = sum(f / (1+tasa)**i for i, f in enumerate(flujos))
                d_npv = sum(-i * f / (1+tasa)**(i+1) for i, f in enumerate(flujos))
            except (OverflowError, ZeroDivisionError):
                return None
            if d_npv == 0: break
            nueva = max(-0.99, min(tasa - npv / d_npv, 10.0))
            if abs(nueva - tasa) < tol: return round(nueva * 100, 2)
            tasa = nueva
        return round(tasa * 100, 2)
    except Exception:
        return None

def _calcular_tir_OLD(flujos, max_iter=200, tol=1e-6):
    if not flujos or all(f == 0 for f in flujos): return None
    if all(f >= 0 for f in flujos) or all(f <= 0 for f in flujos): return None
    tasa = 0.10
    for _ in range(max_iter):
        npv   = sum(f / (1+tasa)**i for i, f in enumerate(flujos))
        d_npv = sum(-i * f / (1+tasa)**(i+1) for i, f in enumerate(flujos))
        if d_npv == 0: break
        nueva = tasa - npv / d_npv
        if abs(nueva - tasa) < tol: return round(nueva * 100, 2)
        tasa = nueva
    return round(tasa * 100, 2)

def calcular_van(flujos, tasa):
    if tasa <= -1: return 0.0
    return sum(f / (1+tasa)**i for i, f in enumerate(flujos))

def get_ppto_key(empresa):
    """Obtiene la clave de PPTO para una empresa de la BD."""
    return EMPRESA_TO_PPTO.get(empresa, '')

def normalizar_sociedad_flujos(empresa_bd):
    """Convierte nombre empresa BD a formato que usa flujos_efectivo."""
    mapeo = {
        "Eficiencia Urbana, S. A.":     "EFICIENCIA URBANA",
        "Capipos, S.A.":                "CAPIPOS",
        "Ottavia, S.A.":                "OTTAVIA",
        "Vilet, S.A.":                  "VILET",
        "Tezzoli, S.A.":                "TEZZOLI",
        "Garbatella, S.A.":             "GARBATELLA",
        "Urviba 2, S.A.":               "URBIVA",
        "Utilica, S.A.":                "UTILICA",
        "Corcolle, S.A.":               "CORCOLLE",
        "Leofreni, S.A.":               "LEOFRENI",
        "Gibraleon, S.A.":              "GIBRALEON",
        "Talocci, S.A.":                "TALOCCI",
        "Ovest, S.A.":                  "OVEST",
        "Rossio, S.A.":                 "ROSSIO",
        "Servicios Generales CCC, S.A.":"SER GEN CCC",
        "Frugalex, S.A.":               "FRUGALEX",
    }
    return mapeo.get(empresa_bd, empresa_bd.upper())

def normalizar_empresa_cartera(empresa_bd):
    mapeo = {
        "Eficiencia Urbana, S. A.":      "Eficiencia Urbana",
        "Capipos, S.A.":                 "Capipos",
        "Ottavia, S.A.":                 "Ottavia",
        "Vilet, S.A.":                   "Vilet",
        "Tezzoli, S.A.":                 "Tezzoli",
        "Garbatella, S.A.":              "Garbatella",
        "Urviba 2, S.A.":                "Urbiva 2",
        "Utilica, S.A.":                 "Utilica",
        "Corcolle, S.A.":                "Corcolle",
        "Leofreni, S.A.":                "Leofreni",
        "Gibraleon, S.A.":               "Gibraleon",
        "Talocci, S.A.":                 "Talocci",
        "Ovest, S.A.":                   "Ovest",
        "Rossio, S.A.":                  "Rossio",
        "Servicios Generales CCC, S.A.": "Servicios Generales",
        "Frugalex, S.A.":                "Frugalex",
    }
    return mapeo.get(empresa_bd, empresa_bd)


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@router.get("/")
async def get_proyectos(db: Session = Depends(get_db), user=Depends(get_current_user)):
    rows = db.execute(text("""
        SELECT DISTINCT p.nombre_sociedad, p.nombre_proyecto
        FROM proyectos p WHERE p.activo = true ORDER BY p.nombre_sociedad
    """)).fetchall()
    return [{"empresa": r.nombre_sociedad, "proyecto": r.nombre_proyecto} for r in rows]


@router.get("/{empresa}")
async def get_supuestos(empresa: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    row = db.execute(text(
        "SELECT * FROM proyeccion_supuestos WHERE empresa = :e LIMIT 1"
    ), {"e": empresa}).fetchone()
    if not row:
        return {"empresa": empresa, "existe": False}
    return {
        "empresa": empresa, "existe": True,
        "ticket_proyectado": float(row.ticket_proyectado or 0),
        "tasa_interes":      float(row.tasa_interes or 0),
        "plazo_meses":       int(row.plazo_meses or 0),
        "anos_proyecto":     int(row.anos_proyecto or 5),
        "tasa_descuento":    float(row.tasa_descuento or 0.12),
        "pct_isr":           float(row.pct_isr or 0),
        "plazos_venta":      row.egresos_operativos or [],  # reutilizamos campo para plazos
        "actualizado_por":   row.actualizado_por,
        "actualizado_en":    str(row.actualizado_en) if row.actualizado_en else None,
    }


@router.post("/{empresa}")
async def save_supuestos(empresa: str, body: dict, db: Session = Depends(get_db), user=Depends(get_current_user)):
    db.execute(text("""
        INSERT INTO proyeccion_supuestos
          (empresa, ticket_proyectado, tasa_interes, plazo_meses,
           anos_proyecto, tasa_descuento, pct_isr,
           egresos_operativos, prestamos, pagos_tierra,
           actualizado_por, actualizado_en)
        VALUES (:e, :tk, :ti, :pm, :ap, :td, :isr,
                CAST(:eo AS jsonb), CAST(:pr AS jsonb), CAST(:pt AS jsonb), :who, NOW())
        ON CONFLICT (empresa) DO UPDATE SET
          ticket_proyectado  = EXCLUDED.ticket_proyectado,
          tasa_interes       = EXCLUDED.tasa_interes,
          plazo_meses        = EXCLUDED.plazo_meses,
          anos_proyecto      = EXCLUDED.anos_proyecto,
          tasa_descuento     = EXCLUDED.tasa_descuento,
          pct_isr            = EXCLUDED.pct_isr,
          egresos_operativos = EXCLUDED.egresos_operativos,
          prestamos          = EXCLUDED.prestamos,
          pagos_tierra       = EXCLUDED.pagos_tierra,
          actualizado_por    = EXCLUDED.actualizado_por,
          actualizado_en     = NOW()
    """), {
        "e":   empresa,
        "tk":  body.get("ticket_proyectado"),
        "ti":  body.get("tasa_interes"),
        "pm":  body.get("plazo_meses"),
        "ap":  body.get("anos_proyecto", 5),
        "td":  body.get("tasa_descuento", 0.12),
        "isr": body.get("pct_isr", 0),
        "eo":  json.dumps(body.get("plazos_venta", [])),
        "pr":  json.dumps(body.get("prestamos_manual", [])),
        "pt":  json.dumps(body.get("pagos_tierra_manual", [])),
        "who": getattr(user, "email", getattr(user, "nombre", "sistema")),
    })
    db.commit()
    return {"ok": True}


@router.get("/{empresa}/horizonte")
async def get_horizonte(empresa: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    """Calcula el horizonte automático basado en la última fecha de cobro en cartera."""
    row = db.execute(text("""
        SELECT
            MAX(EXTRACT(YEAR FROM fecha_programada_cobro)) AS ultimo_anio,
            MIN(EXTRACT(YEAR FROM fecha_programada_cobro)) AS primer_anio
        FROM ov_cartera
        WHERE empresa = :e
          AND line_status = 'O'
          AND fecha_programada_cobro IS NOT NULL
    """), {"e": normalizar_empresa_cartera(empresa)}).fetchone()

    anio_actual = date.today().year
    ultimo_anio = int(row.ultimo_anio or anio_actual)
    horizonte_calc = max(1, ultimo_anio - anio_actual + 1)

    return {
        "empresa": empresa,
        "ultimo_anio_ov": ultimo_anio,
        "anio_actual": anio_actual,
        "horizonte_calculado": horizonte_calc,
    }


@router.get("/{empresa}/ingresos-por-anio")
async def get_ingresos_por_anio(empresa: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    """Distribución anual de ingresos reales por fecha_programada_cobro."""
    rows = db.execute(text("""
        SELECT
            EXTRACT(YEAR FROM fecha_programada_cobro) AS anio,
            SUM(saldo_pendiente)                       AS total
        FROM ov_cartera
        WHERE empresa = :e
          AND line_status = 'O'
          AND fecha_programada_cobro IS NOT NULL
        GROUP BY anio
        ORDER BY anio
    """), {"e": normalizar_empresa_cartera(empresa)}).fetchall()

    return {
        "empresa": empresa,
        "distribucion": [{"anio": int(r.anio), "total": float(r.total)} for r in rows],
    }


@router.get("/{empresa}/ingresos")
async def get_ingresos(
    empresa: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    """Ingresos reales = saldo pendiente cartera + ingresos proyectados por plazos de venta."""

    # 1. Saldo pendiente real de cartera (ov_cartera)
    rr = db.execute(text("""
        SELECT
            COUNT(DISTINCT doc_num)                         AS contratos,
            COALESCE(SUM(saldo_pendiente), 0)               AS saldo_pendiente_total,
            COALESCE(SUM(CASE WHEN tipo_linea = 'BB' THEN saldo_pendiente ELSE 0 END), 0) AS saldo_capital,
            COALESCE(SUM(CASE WHEN tipo_linea = 'S'  THEN saldo_pendiente ELSE 0 END), 0) AS saldo_interes
        FROM ov_cartera
        WHERE empresa = :e
          AND line_status = 'O'
    """), {"e": normalizar_empresa_cartera(empresa)}).fetchone()

    ingresos_reales = {
        "contratos":        int(rr.contratos or 0),
        "saldo_pendiente":  float(rr.saldo_pendiente_total or 0),
        "saldo_capital":    float(rr.saldo_capital or 0),
        "saldo_interes":    float(rr.saldo_interes or 0),
    }

    # 2. Lotes disponibles (para tabla de plazos proyectados)
    lotes = db.execute(text("""
        SELECT l.unidad_key, p.nombre_proyecto,
               COALESCE(l.precio_con_descuento, l.precio_sin_descuento, 0) AS precio
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE p.nombre_sociedad = :e AND l.estatus = 'DISPONIBLE'
        ORDER BY l.unidad_key
    """), {"e": empresa}).fetchall()

    disponibles = [{"lote": r.unidad_key, "proyecto": r.nombre_proyecto,
                    "precio": float(r.precio)} for r in lotes]

    return {
        "empresa": empresa,
        "ingresos_reales": ingresos_reales,
        "disponibles": disponibles,
        "total_disponibles": len(disponibles),
        "precio_promedio": round(sum(d["precio"] for d in disponibles) / len(disponibles), 2) if disponibles else 0,
    }


@router.get("/{empresa}/egresos-operativos")
async def get_egresos_operativos(
    empresa: str,
    anos_egr: int = Query(0, description="Años para distribuir egresos pendientes (0=no distribuir)"),
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    """Presupuesto vs Ejecutado para Urbanización y Administración."""
    ppto_key = get_ppto_key(empresa)
    ppto = PPTO_PROYECTOS.get(ppto_key, {"urb": 0, "adm": 0, "total": 0})
    sociedad_flujos = normalizar_sociedad_flujos(empresa)

    # Ejecutado base desde flujos_efectivo
    rows = db.execute(text("""
        SELECT seccion, COALESCE(SUM(monto_egreso), 0) AS ejecutado
        FROM flujos_efectivo
        WHERE sociedad ILIKE :s
          AND seccion IN ('EGRESOS / URBANIZACION', 'EGRESOS / ADMINISTRACION')
        GROUP BY seccion
    """), {"s": f"%{sociedad_flujos}%"}).fetchall()

    ejecutado = {"EGRESOS / URBANIZACION": 0.0, "EGRESOS / ADMINISTRACION": 0.0}
    for r in rows:
        ejecutado[r.seccion] = float(r.ejecutado)

    # Aplicar reclasificaciones (misma lógica que flujos.py)
    reclas = db.execute(text("""
        SELECT seccion_origen, seccion_destino, SUM(monto) AS monto
        FROM flujos_reclasificaciones
        WHERE sociedad ILIKE :s
          AND (seccion_origen IN ('EGRESOS / URBANIZACION','EGRESOS / ADMINISTRACION')
           OR  seccion_destino IN ('EGRESOS / URBANIZACION','EGRESOS / ADMINISTRACION'))
        GROUP BY seccion_origen, seccion_destino
    """), {"s": f"%{sociedad_flujos}%"}).fetchall()

    for r in reclas:
        ori, dst, monto = r.seccion_origen, r.seccion_destino, float(r.monto or 0)
        if not monto or ori == dst:
            continue
        if ori in ejecutado:
            ejecutado[ori] -= monto
        if dst in ejecutado:
            ejecutado[dst] += monto

    ejec_urb   = max(ejecutado["EGRESOS / URBANIZACION"],   0.0)
    ejec_adm   = max(ejecutado["EGRESOS / ADMINISTRACION"], 0.0)
    ejec_total = ejec_urb + ejec_adm

    ppto_urb = ppto["urb"]
    ppto_adm = ppto["adm"]
    ppto_total = ppto["total"]

    pendiente_urb = round(max(ppto_urb - ejec_urb, 0), 2)
    pendiente_adm = round(max(ppto_adm - ejec_adm, 0), 2)
    pendiente_total = round(max(ppto_total - ejec_total, 0), 2)

    # Distribución anual del pendiente si se solicitó
    dist_urb = round(pendiente_urb / anos_egr, 2) if anos_egr > 0 else None
    dist_adm = round(pendiente_adm / anos_egr, 2) if anos_egr > 0 else None
    dist_total = round(pendiente_total / anos_egr, 2) if anos_egr > 0 else None

    return {
        "empresa": empresa,
        "ppto_key": ppto_key,
        "anos_distribucion": anos_egr,
        "urbanizacion": {
            "presupuesto": round(ppto_urb, 2),
            "ejecutado":   round(ejec_urb, 2),
            "pendiente":   pendiente_urb,
            "avance_pct":  round(ejec_urb / ppto_urb * 100, 1) if ppto_urb else 0,
            "dist_anual":  dist_urb,
        },
        "administracion": {
            "presupuesto": round(ppto_adm, 2),
            "ejecutado":   round(ejec_adm, 2),
            "pendiente":   pendiente_adm,
            "avance_pct":  round(ejec_adm / ppto_adm * 100, 1) if ppto_adm else 0,
            "dist_anual":  dist_adm,
        },
        "total": {
            "presupuesto": round(ppto_total, 2),
            "ejecutado":   round(ejec_total, 2),
            "pendiente":   pendiente_total,
            "avance_pct":  round(ejec_total / ppto_total * 100, 1) if ppto_total else 0,
            "dist_anual":  dist_total,
        },
    }


@router.get("/{empresa}/egresos-financieros")
async def get_egresos_financieros(
    empresa: str,
    anos_ic: int = Query(0, description="Años para distribuir intercompany (0=no distribuir)"),
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    """Préstamos bancarios (tabla amortización vs pagado) e Intercompany."""
    sociedad_flujos = normalizar_sociedad_flujos(empresa)
    hoy = date.today()

    # ── PRÉSTAMO BANCARIO ──
    prestamo_info = PRESTAMOS_AMORT.get(empresa)
    prestamo_result = None

    if prestamo_info:
        # Pagado real desde flujos_efectivo (Prestamo Bancario)
        pagado_row = db.execute(text("""
            SELECT COALESCE(SUM(monto_egreso), 0) AS pagado
            FROM flujos_efectivo
            WHERE sociedad ILIKE :s
              AND seccion = 'FINANCIAMIENTO'
              AND nombre_categoria = 'Prestamo Bancario'
        """), {"s": f"%{sociedad_flujos}%"}).fetchone()
        pagado_total = float(pagado_row.pagado or 0)

        cuotas = prestamo_info.get("cuotas", [])
        monto_original = prestamo_info["monto_original"]

        # Determinar cuotas pagadas usando monto real pagado en BD
        # Acumular cuotas hasta cubrir pagado_total real
        capital_pagado_tabla = 0.0
        interes_pagado_tabla = 0.0
        cuotas_pagadas = []
        cuotas_pendientes = []
        monto_cubierto = 0.0
        cuotas_pendientes_raw = []
        for c in cuotas:
            cuota_total_c = c["capital"] + c["interes"]
            if monto_cubierto + cuota_total_c <= pagado_total + 0.01:
                capital_pagado_tabla += c["capital"]
                interes_pagado_tabla += c["interes"]
                monto_cubierto += cuota_total_c
                cuotas_pagadas.append(c)
            else:
                cuotas_pendientes_raw.append(c)

        # Separar vencidas (fecha <= hoy) de futuras y acumular vencidas en un solo pago
        cuotas_vencidas = [c for c in cuotas_pendientes_raw
                           if datetime.strptime(c["fecha"], "%Y-%m-%d").date() <= hoy]
        cuotas_futuras  = [c for c in cuotas_pendientes_raw
                           if datetime.strptime(c["fecha"], "%Y-%m-%d").date() > hoy]

        cuotas_pendientes = []
        if cuotas_vencidas:
            cap_venc = sum(c["capital"] for c in cuotas_vencidas)
            int_venc = sum(c["interes"]  for c in cuotas_vencidas)
            prox_fecha = cuotas_futuras[0]["fecha"] if cuotas_futuras else hoy.replace(day=5).strftime("%Y-%m-%d")
            cuotas_pendientes.append({
                "op": "VENCIDO",
                "fecha": prox_fecha,
                "saldo_capital": cuotas_vencidas[0]["saldo_capital"] if cuotas_vencidas else 0,
                "capital": round(cap_venc, 2),
                "interes": round(int_venc, 2),
                "cuota": round(cap_venc + int_venc, 2),
                "es_vencido": True,
                "cuotas_acumuladas": len(cuotas_vencidas),
            })
        cuotas_pendientes.extend(cuotas_futuras)

        pendiente_capital = sum(c["capital"] for c in cuotas_pendientes)
        pendiente_interes = sum(c["interes"] for c in cuotas_pendientes)

        prestamo_result = {
            "banco":             prestamo_info["banco"],
            "no_credito":        prestamo_info["no_credito"],
            "monto_original":    monto_original,
            "tasa":              prestamo_info["tasa"],
            "pagado_capital":    round(capital_pagado_tabla, 2),
            "pagado_interes":    round(interes_pagado_tabla, 2),
            "pagado_total":      round(pagado_total, 2),
            "pendiente_capital": round(pendiente_capital, 2),
            "pendiente_interes": round(pendiente_interes, 2),
            "pendiente_total":   round(pendiente_capital + pendiente_interes, 2),
            "cuotas_pagadas":    len(cuotas_pagadas),
            "cuotas_pendientes": cuotas_pendientes,  # lista completa para mostrar tabla
        }

    # ── INTERCOMPANY ──
    ic_row = db.execute(text("""
        SELECT COALESCE(SUM(monto_egreso), 0) AS egresado,
               COALESCE(SUM(monto_ingreso), 0) AS ingresado
        FROM flujos_efectivo
        WHERE sociedad ILIKE :s
          AND seccion = 'FINANCIAMIENTO'
          AND nombre_categoria = 'Intercompany'
    """), {"s": f"%{sociedad_flujos}%"}).fetchone()

    saldo_intercompany = float(ic_row.egresado or 0) - float(ic_row.ingresado or 0)
    ic_tipo = "por pagar" if saldo_intercompany > 0 else "a favor"
    ic_abs  = abs(saldo_intercompany)
    ic_dist = round(ic_abs / anos_ic, 2) if anos_ic > 0 else None

    return {
        "empresa": empresa,
        "prestamo_bancario": prestamo_result,
        "intercompany": {
            "saldo": round(ic_abs, 2),
            "tipo":  ic_tipo,
            "anos_distribucion": anos_ic,
            "dist_anual": ic_dist,
        },
    }


@router.get("/{empresa}/tierra-dividendos")
async def get_tierra_dividendos(
    empresa: str,
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    """Plan de pagos tierra y dividendos: plan total vs pagado vs pendiente."""
    sociedad_flujos = normalizar_sociedad_flujos(empresa)
    hoy = date.today()

    # Pagado real de TERRENO
    tierra_pagado_row = db.execute(text("""
        SELECT COALESCE(SUM(monto_egreso), 0) AS pagado
        FROM flujos_efectivo
        WHERE sociedad ILIKE :s AND seccion = 'TERRENO'
    """), {"s": f"%{sociedad_flujos}%"}).fetchone()
    tierra_pagado = float(tierra_pagado_row.pagado or 0)

    # Pagado real de DIVIDENDOS
    div_pagado_row = db.execute(text("""
        SELECT COALESCE(SUM(monto_egreso), 0) AS pagado
        FROM flujos_efectivo
        WHERE sociedad ILIKE :s AND seccion = 'DIVIDENDOS'
    """), {"s": f"%{sociedad_flujos}%"}).fetchone()
    div_pagado = float(div_pagado_row.pagado or 0)

    # Plan tierra para esta sociedad
    soc_upper = sociedad_flujos.upper().strip()
    plan_tierra_empresa = []
    for plan in PLAN_TIERRA:
        plan_soc = plan["sociedad"].upper().strip()
        if plan_soc == soc_upper or plan_soc in soc_upper or soc_upper in plan_soc:
            plan_tierra_empresa.append(plan)

    # Calcular total plan tierra y pendientes
    tierra_plan_total = 0.0
    tierra_pendiente_detalle = []
    for plan in plan_tierra_empresa:
        for fecha_str, monto in plan["pagos"].items():
            tierra_plan_total += monto
            fecha_p = datetime.strptime(fecha_str, "%Y-%m-%d").date()
            if fecha_p > hoy:
                tierra_pendiente_detalle.append({
                    "fecha": fecha_str,
                    "concepto": plan["concepto"],
                    "monto": round(monto, 2)
                })

    tierra_pendiente_detalle.sort(key=lambda x: x["fecha"])
    tierra_pendiente_total = sum(p["monto"] for p in tierra_pendiente_detalle)

    # Plan dividendos para esta sociedad
    plan_div_empresa = []
    for plan in PLAN_DIVIDENDOS:
        plan_soc = plan["sociedad"].upper().strip()
        if plan_soc == soc_upper or plan_soc in soc_upper or soc_upper in plan_soc:
            plan_div_empresa.append(plan)

    div_plan_total = 0.0
    div_pendiente_detalle = []
    for plan in plan_div_empresa:
        for fecha_str, monto in plan["pagos"].items():
            div_plan_total += monto
            fecha_p = datetime.strptime(fecha_str, "%Y-%m-%d").date()
            if fecha_p > hoy:
                div_pendiente_detalle.append({
                    "fecha": fecha_str,
                    "cuenta": plan["cuenta"],
                    "monto": round(monto, 2)
                })

    div_pendiente_detalle.sort(key=lambda x: x["fecha"])
    div_pendiente_total = sum(p["monto"] for p in div_pendiente_detalle)

    return {
        "empresa": empresa,
        "tierra": {
            "plan_total":   round(tierra_plan_total, 2),
            "pagado":       round(tierra_pagado, 2),
            "pendiente":    round(tierra_pendiente_total, 2),
            "avance_pct":   round(tierra_pagado / tierra_plan_total * 100, 1) if tierra_plan_total else 0,
            "proximos_pagos": tierra_pendiente_detalle[:24],  # próximos 24 meses
        },
        "dividendos": {
            "plan_total":   round(div_plan_total, 2),
            "pagado":       round(div_pagado, 2),
            "pendiente":    round(div_pendiente_total, 2),
            "avance_pct":   round(div_pagado / div_plan_total * 100, 1) if div_plan_total else 0,
            "proximos_pagos": div_pendiente_detalle[:24],
        },
    }


@router.get("/{empresa}/flujo")
async def get_flujo(empresa: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    """Flujo proyectado al cierre — consolida todos los módulos."""
    sup = db.execute(
        text("SELECT * FROM proyeccion_supuestos WHERE empresa = :e LIMIT 1"),
        {"e": empresa}
    ).fetchone()
    if not sup:
        raise HTTPException(404,
            "No hay supuestos guardados. Completá los datos y presioná 'Guardar cambios'.")

    anos = int(sup.anos_proyecto or 5)
    tasa_desc = float(sup.tasa_descuento or 0.12)
    pct_isr = float(sup.pct_isr or 0)
    plazos = sup.egresos_operativos or []  # plazos_venta guardados

    # Llamar a los sub-endpoints internamente
    ing  = await get_ingresos(empresa, db, user)
    egr  = await get_egresos_operativos(empresa, db, user)
    fin  = await get_egresos_financieros(empresa, db, user)
    tier = await get_tierra_dividendos(empresa, db, user)

    # Calcular ingresos proyectados desde plazos
    total_ing_proyectado = 0.0
    for p in plazos:
        ini_str = p.get("inicio", "")
        fin_str = p.get("fin", "")
        tk      = float(p.get("ticket", 0))
        tasa_p  = float(p.get("tasa", 0))
        plazo_m = int(p.get("plazo", 0)) if str(p.get("plazo","")).isdigit() else 0
        unid_mes= int(p.get("unidMes", 0))
        if not ini_str or not fin_str or not tk or not unid_mes: continue
        interes_u = tk * (tasa_p/100) * (plazo_m/12) if plazo_m > 0 else 0
        total_ing_proyectado += (tk + interes_u) * int(p.get("unidProy", unid_mes))

    total_ing_real = ing["ingresos_reales"]["saldo_pendiente"]
    total_ing = total_ing_real + total_ing_proyectado

    # IVA
    cap_real = ing["ingresos_reales"]["saldo_capital"]
    cap_proy = sum(float(p.get("ticket", 0)) * int(p.get("unidProy", int(p.get("unidMes",0)))) for p in plazos)
    iva_deb  = (cap_real + cap_proy) * 0.70 * 0.12
    egr_op_total = egr["total"]["pendiente"]
    iva_cred = egr_op_total * 0.12
    iva_neto = iva_deb - iva_cred

    # Financiero
    prest = fin.get("prestamo_bancario") or {}
    egr_prest = prest.get("pendiente_total", 0)
    egr_ic = fin["intercompany"]["saldo"]
    egr_fin = egr_prest + egr_ic

    # Tierra y dividendos
    egr_tierra = tier["tierra"]["pendiente"]
    egr_div = tier["dividendos"]["pendiente"]

    # ISR
    isr = total_ing * pct_isr

    flujo_neto = total_ing - egr_op_total - iva_neto - egr_fin - isr - egr_tierra - egr_div

    # ── Distribución anual de ingresos reales por fecha_programada_cobro ──
    anio_actual = date.today().year
    ing_real_rows = db.execute(text("""
        SELECT EXTRACT(YEAR FROM fecha_programada_cobro) AS anio,
               SUM(saldo_pendiente) AS total
        FROM ov_cartera
        WHERE empresa = :e AND line_status = 'O'
          AND fecha_programada_cobro IS NOT NULL
        GROUP BY anio ORDER BY anio
    """), {"e": normalizar_empresa_cartera(empresa)}).fetchall()
    ing_real_por_anio = {int(r.anio): float(r.total) for r in ing_real_rows}

    # ── Distribución anual de ingresos proyectados por plazos ──
    ing_proy_por_anio = {}
    for p in plazos:
        ini_str = p.get("inicio", "")
        fin_str = p.get("fin", "")
        tk      = float(p.get("ticket", 0))
        tasa_p  = float(p.get("tasa", 0))
        plazo_m = int(p.get("plazo", 0)) if str(p.get("plazo","")).isdigit() else 0
        unid_mes= int(p.get("unidMes", 0))
        if not ini_str or not fin_str or not tk or not unid_mes: continue
        try:
            ini_d = datetime.strptime(ini_str + "-01", "%Y-%m-%d").date()
            fin_d = datetime.strptime(fin_str + "-01", "%Y-%m-%d").date()
        except: continue
        interes_u = tk * (tasa_p/100) * (plazo_m/12) if plazo_m > 0 else 0
        ingreso_u = tk + interes_u
        cur = ini_d
        while cur < fin_d:
            yr_p = cur.year
            ing_proy_por_anio[yr_p] = ing_proy_por_anio.get(yr_p, 0) + ingreso_u * unid_mes
            mes = cur.month + 1
            anio_c = cur.year + (1 if mes > 12 else 0)
            mes = mes if mes <= 12 else 1
            try: cur = cur.replace(year=anio_c, month=mes)
            except: break

    # Determinar rango real de años
    todos_anios = set(ing_real_por_anio.keys()) | set(ing_proy_por_anio.keys())
    if todos_anios:
        anio_max = max(todos_anios)
        anos_real = max(1, anio_max - anio_actual + 1)
    else:
        anos_real = anos
    anos_total = max(anos_real, anos)

    # Egresos distribuidos sobre el horizonte del usuario
    egr_a  = egr_op_total / anos if anos else egr_op_total
    iva_a  = iva_neto / anos if anos else iva_neto
    isr_a  = isr / anos if anos else isr
    tier_a = (egr_tierra + egr_div) / anos if anos else (egr_tierra + egr_div)

    # Cuotas préstamo por año calendario
    cuotas_pend = prest.get("cuotas_pendientes", [])
    hoy = date.today()
    fin_yr = {}
    for c in cuotas_pend:
        try:
            yr = int(c["fecha"][:4]) - anio_actual + 1
            yr = max(1, yr)
            fin_yr[yr] = fin_yr.get(yr, 0) + c["cuota"]
        except: pass
    fin_yr_ic = egr_ic / anos if anos else egr_ic

    flujo_anual = []
    pico = {"anio": None, "flujo": 0.0}
    for yr in range(1, anos_total + 1):
        anio_cal = anio_actual + yr - 1
        ing_real_yr = ing_real_por_anio.get(anio_cal, 0)
        ing_proy_yr = ing_proy_por_anio.get(anio_cal, 0)
        ing_yr = ing_real_yr + ing_proy_yr

        if yr <= anos:
            egr_yr  = egr_a
            iva_yr  = iva_a
            isr_yr  = isr_a
            tier_yr = tier_a
        else:
            egr_yr = iva_yr = isr_yr = tier_yr = 0.0

        efin_yr = fin_yr.get(yr, 0) + (fin_yr_ic if yr <= anos else 0)
        fn_yr = ing_yr - egr_yr - iva_yr - efin_yr - isr_yr - tier_yr
        acum  = (flujo_anual[-1]["flujo_acumulado"] if flujo_anual else 0) + fn_yr
        flujo_anual.append({
            "anio": yr, "anio_cal": anio_cal,
            "ingresos": round(ing_yr, 2),
            "ing_real": round(ing_real_yr, 2),
            "ing_proy": round(ing_proy_yr, 2),
            "egresos_op": round(egr_yr, 2), "iva_neto": round(iva_yr, 2),
            "egresos_fin": round(efin_yr, 2), "isr": round(isr_yr, 2),
            "tierra_capital": round(tier_yr, 2), "flujo_neto": round(fn_yr, 2),
            "flujo_acumulado": round(acum, 2), "es_negativo": fn_yr < 0,
        })
        if fn_yr < pico["flujo"]:
            pico = {"anio": yr, "flujo": round(fn_yr, 2)}

    inv0 = -(egr_tierra + egr_div + prest.get("pendiente_capital", 0))
    flujos_calc = [inv0] + [f["flujo_neto"] for f in flujo_anual]
    tir = calcular_tir(flujos_calc)
    van = calcular_van(flujos_calc, tasa_desc)

    u_total = ing["total_disponibles"] + ing["ingresos_reales"]["contratos"]
    ticket_prom = total_ing / u_total if u_total else 0
    lotes_pe = int((egr_op_total + iva_neto + egr_fin + isr + egr_tierra + egr_div) / ticket_prom) if ticket_prom > 0 else 0
    margen = round(flujo_neto / total_ing * 100, 2) if total_ing else 0

    return {
        "empresa": empresa,
        "resumen_ingresos": {"real": total_ing_real, "proyectado": total_ing_proyectado, "total": total_ing},
        "resumen_egresos_op": egr["total"],
        "iva": {"debito": round(iva_deb,2), "credito": round(iva_cred,2), "neto": round(iva_neto,2)},
        "egresos_financieros": {"prestamo": egr_prest, "intercompany": egr_ic, "total": round(egr_fin,2)},
        "isr": {"pct": pct_isr, "total": round(isr, 2)},
        "tierra_dividendos": {"tierra": egr_tierra, "dividendos": egr_div, "total": round(egr_tierra+egr_div,2)},
        "flujo_neto_total": round(flujo_neto, 2),
        "flujo_anual": flujo_anual,
        "indicadores": {
            "tir": tir, "van": round(van,2), "margen_neto": margen,
            "punto_equilibrio_lotes": lotes_pe,
            "pico_negativo": pico if pico["anio"] else None,
        },
    }

@router.delete("/{empresa}")
async def delete_supuestos(empresa: str, db: Session = Depends(get_db), user=Depends(get_current_user)):
    """Elimina los supuestos guardados de un proyecto para empezar de cero."""
    db.execute(text("DELETE FROM proyeccion_supuestos WHERE empresa = :e"), {"e": empresa})
    db.commit()
    return {"ok": True, "empresa": empresa, "mensaje": "Supuestos eliminados correctamente"}

@router.get("/cxp")
async def get_cuentas_por_pagar(db: Session = Depends(get_db), user=Depends(get_current_user)):
    """
    Cuentas por pagar consolidadas — datos completos sin filtro de fecha.
    Visualización histórica completa para Junta Directiva.
    """
    try:
        rows = db.execute(text("""
            SELECT empresa, doc_num, codigo_proveedor, nombre_acreedor,
                   ref_acreedor, fecha_vencimiento, importe, comentarios
            FROM cuentas_por_pagar
            ORDER BY empresa, fecha_vencimiento
        """)).fetchall()

        items = []
        total = 0.0
        for r in rows:
            imp = float(r.importe or 0)
            total += imp
            items.append({
                "empresa": r.empresa,
                "doc_num": str(r.doc_num) if r.doc_num else None,
                "codigo_proveedor": r.codigo_proveedor,
                "nombre_acreedor": r.nombre_acreedor,
                "ref_acreedor": r.ref_acreedor,
                "fecha_vencimiento": str(r.fecha_vencimiento)[:10] if r.fecha_vencimiento else None,
                "importe": imp,
                "comentarios": r.comentarios,
            })

        # Resumen por empresa (para totales en frontend)
        resumen_empresa = {}
        for it in items:
            e = it["empresa"] or "—"
            resumen_empresa.setdefault(e, {"empresa": e, "total": 0.0, "count": 0})
            resumen_empresa[e]["total"] += it["importe"]
            resumen_empresa[e]["count"] += 1

        return {
            "items": items,
            "total_general": total,
            "total_registros": len(items),
            "por_empresa": sorted(resumen_empresa.values(), key=lambda x: -x["total"]),
        }
    except Exception as e:
        # Si la tabla no existe (sync nunca ejecutado) devolver vacío sin error
        return {"items": [], "total_general": 0, "total_registros": 0, "por_empresa": [], "_warning": str(e)[:200]}


@router.post("/cxp/sync")
async def sync_cuentas_por_pagar(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user=Depends(get_current_user)
):
    """
    Sincroniza la tabla cuentas_por_pagar desde la pestaña 'CUENTA POR PAGAR'
    del Excel 'PRESUPUESTO, PRESTAMOS Y TIERRA.xlsx'.
    El usuario sube el archivo, el endpoint procesa y reemplaza los registros.
    """
    # Validar archivo
    if not file.filename or not file.filename.lower().endswith(('.xlsx', '.xlsm')):
        raise HTTPException(status_code=400, detail="Archivo debe ser .xlsx o .xlsm")

    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas no instalado en el backend")

    content = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo abrir el Excel: {e}")

    if 'CUENTA POR PAGAR' not in xl.sheet_names:
        raise HTTPException(
            status_code=400,
            detail=f"Pestaña 'CUENTA POR PAGAR' no encontrada. Pestañas disponibles: {xl.sheet_names}"
        )

    df = pd.read_excel(xl, sheet_name='CUENTA POR PAGAR', header=0)
    df.columns = [str(c).strip() for c in df.columns]

    # Validar columnas requeridas
    required = ['EMPRESA', 'Nº documento', 'Código de proveedor', 'Nombre de acreedor',
                'No.Ref.del acreedor', 'Fecha de vencimiento', 'Importe', 'Comentarios']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Columnas faltantes en la pestaña: {missing}. Encontradas: {list(df.columns)}"
        )

    # Filtrar filas vacías
    df = df.dropna(subset=['EMPRESA', 'Importe'], how='all')
    df = df[df['EMPRESA'].astype(str).str.strip() != '']

    # Asegurar que la tabla existe
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS cuentas_por_pagar (
            id SERIAL PRIMARY KEY,
            empresa VARCHAR(150),
            doc_num VARCHAR(50),
            codigo_proveedor VARCHAR(50),
            nombre_acreedor VARCHAR(300),
            ref_acreedor VARCHAR(150),
            fecha_vencimiento DATE,
            importe NUMERIC(14,2),
            comentarios TEXT,
            sincronizado_en TIMESTAMP DEFAULT NOW()
        )
    """))

    # Limpiar tabla antes de re-cargar (full refresh)
    db.execute(text("TRUNCATE TABLE cuentas_por_pagar RESTART IDENTITY"))

    # Insertar
    insertados = 0
    errores = []
    for idx, row in df.iterrows():
        try:
            fv = row['Fecha de vencimiento']
            if pd.isna(fv):
                fv_str = None
            elif hasattr(fv, 'date'):
                fv_str = fv.date()
            else:
                fv_str = pd.to_datetime(fv).date()

            db.execute(text("""
                INSERT INTO cuentas_por_pagar
                  (empresa, doc_num, codigo_proveedor, nombre_acreedor,
                   ref_acreedor, fecha_vencimiento, importe, comentarios)
                VALUES
                  (:e, :dn, :cp, :na, :ra, :fv, :imp, :com)
            """), {
                "e":   str(row['EMPRESA']).strip()[:150] if pd.notna(row['EMPRESA']) else None,
                "dn":  str(row['Nº documento']).strip()[:50] if pd.notna(row['Nº documento']) else None,
                "cp":  str(row['Código de proveedor']).strip()[:50] if pd.notna(row['Código de proveedor']) else None,
                "na":  str(row['Nombre de acreedor']).strip()[:300] if pd.notna(row['Nombre de acreedor']) else None,
                "ra":  str(row['No.Ref.del acreedor']).strip()[:150] if pd.notna(row['No.Ref.del acreedor']) else None,
                "fv":  fv_str,
                "imp": float(row['Importe']) if pd.notna(row['Importe']) else 0,
                "com": str(row['Comentarios']).strip() if pd.notna(row['Comentarios']) else None,
            })
            insertados += 1
        except Exception as e:
            errores.append({"fila": int(idx) + 2, "error": str(e)[:200]})

    db.commit()

    return {
        "ok": True,
        "insertados": insertados,
        "errores": errores[:20],  # primeros 20 errores si hay
        "total_errores": len(errores),
        "mensaje": f"Sincronizados {insertados} registros de cuentas por pagar"
    }
