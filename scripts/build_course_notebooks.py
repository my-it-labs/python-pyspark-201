#!/usr/bin/env python3
"""Genera todos los notebooks de curso (teoría + labs-guion) bajo notebooks/M0x/."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from nb_course.common import NB, write_notebook  # noqa: E402
from nb_course import labs_early, labs_late, m00, m08, teoria  # noqa: E402

SPECS = [
    ("M00-entorno-notebooks/01-teoria.ipynb", m00.teoria),
    ("M00-entorno-notebooks/02-lab-primer-notebook.ipynb", m00.lab),
    ("M00-entorno-notebooks/03-python-recordatorio.ipynb", m00.python_sheet),
    ("M01-fundamentos-entorno/01-teoria.ipynb", teoria.m01),
    ("M01-fundamentos-entorno/02-lab-sesion-spark.ipynb", labs_early.m01_01),
    ("M02-ingesta-preparacion/01-teoria.ipynb", teoria.m02),
    ("M02-ingesta-preparacion/02-lab-ingesta-csv-json.ipynb", labs_early.m02_01),
    ("M02-ingesta-preparacion/03-lab-schema-tipos.ipynb", labs_early.m02_02),
    ("M02-ingesta-preparacion/04-lab-calidad-limpieza.ipynb", labs_early.m02_03),
    ("M03-transformacion-datos/01-teoria.ipynb", teoria.m03),
    ("M03-transformacion-datos/02-lab-enriquecimiento.ipynb", labs_early.m03_01),
    ("M03-transformacion-datos/03-lab-reglas-negocio.ipynb", labs_early.m03_02),
    ("M04-integracion-agregacion/01-teoria.ipynb", teoria.m04),
    ("M04-integracion-agregacion/02-lab-joins.ipynb", labs_late.m04_01),
    ("M04-integracion-agregacion/03-lab-kpis.ipynb", labs_late.m04_02),
    ("M04-integracion-agregacion/04-lab-segmentacion.ipynb", labs_late.m04_03),
    ("M05-analisis-avanzado/01-teoria.ipynb", teoria.m05),
    ("M05-analisis-avanzado/02-lab-ranking-ventana.ipynb", labs_late.m05_01),
    ("M05-analisis-avanzado/03-lab-acumulados.ipynb", labs_late.m05_02),
    ("M06-optimizacion-ejecucion/01-teoria.ipynb", teoria.m06),
    ("M06-optimizacion-ejecucion/02-lab-explain-dag.ipynb", labs_late.m06_01),
    ("M06-optimizacion-ejecucion/03-lab-cache-particionado.ipynb", labs_late.m06_02),
    ("M07-persistencia-datos/01-teoria.ipynb", teoria.m07),
    ("M07-persistencia-datos/02-lab-parquet-layout.ipynb", labs_late.m07_01),
    ("M08-json-anidado-schema/01-teoria.ipynb", m08.teoria),
    ("M08-json-anidado-schema/02-lab-json-anidado-schema.ipynb", m08.lab),
]


def build_all() -> None:
    for rel, factory in SPECS:
        dest = NB / rel
        write_notebook(dest, factory())
        print("wrote", dest.relative_to(ROOT))


if __name__ == "__main__":
    build_all()
