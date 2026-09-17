# Notebooks

El curso **es** esta carpeta. Cada módulo tiene su directorio. Tú escribes en `trabajo/`.

| Carpeta | Qué haces |
|---------|-----------|
| `M00` … `M07` | Abres `01-teoria.ipynb` (ejecutas aquí) y los `0N-lab-….ipynb` (guion: **creas el tuyo**). |
| `M08` | Extra: JSON anidado y schema legacy. No bloquea el pipeline. |
| [`trabajo/`](trabajo/README.md) | **Tu** sitio. Un `.ipynb` por lab, con celdas Markdown + código. |
| `_qa/` | No lo uses. Batería interna del repo. |

Kernel: **Python (NovaShop)**.

## Directorio

| Módulo | Notebooks |
|--------|-----------|
| [M00 — Entorno](M00-entorno-notebooks/README.md) | [teoría](M00-entorno-notebooks/01-teoria.ipynb) · [lab](M00-entorno-notebooks/02-lab-primer-notebook.ipynb) · [Python](M00-entorno-notebooks/03-python-recordatorio.ipynb) |
| [M01 — Fundamentos](M01-fundamentos-entorno/README.md) | [teoría](M01-fundamentos-entorno/01-teoria.ipynb) · [lab](M01-fundamentos-entorno/02-lab-sesion-spark.ipynb) |
| [M02 — Ingesta](M02-ingesta-preparacion/README.md) | [teoría](M02-ingesta-preparacion/01-teoria.ipynb) · [labs](M02-ingesta-preparacion/README.md) |
| [M03 — Transformación](M03-transformacion-datos/README.md) | [teoría](M03-transformacion-datos/01-teoria.ipynb) · [labs](M03-transformacion-datos/README.md) |
| [M04 — Joins y KPIs](M04-integracion-agregacion/README.md) | [teoría](M04-integracion-agregacion/01-teoria.ipynb) · [labs](M04-integracion-agregacion/README.md) |
| [M05 — Ventanas](M05-analisis-avanzado/README.md) | [teoría](M05-analisis-avanzado/01-teoria.ipynb) · [labs](M05-analisis-avanzado/README.md) |
| [M06 — Ejecución](M06-optimizacion-ejecucion/README.md) | [teoría](M06-optimizacion-ejecucion/01-teoria.ipynb) · [labs](M06-optimizacion-ejecucion/README.md) |
| [M07 — Parquet](M07-persistencia-datos/README.md) | [teoría](M07-persistencia-datos/01-teoria.ipynb) · [lab](M07-persistencia-datos/02-lab-parquet-layout.ipynb) |
| [M08 — JSON anidado (extra)](M08-json-anidado-schema/README.md) | [teoría](M08-json-anidado-schema/01-teoria.ipynb) · [lab](M08-json-anidado-schema/02-lab-json-anidado-schema.ipynb) |

## Tu notebook (todos los labs)

Nombre exacto: el que indica el guion (tabla de cada módulo). Carpeta: `notebooks/trabajo/`.

En cada paso: **Markdown** (qué y por qué) → **código** → `Shift+Enter` → comprueba la salida → si no cuadra, mejora.

Celda de arranque (cópiala del guion; es la misma en todos):

```python
import sys
from pathlib import Path

_here = Path.cwd().resolve()
ROOT = next(
    p
    for p in [_here, *_here.parents]
    if (p / "labs" / "_shared" / "session.py").is_file()
)
sys.path.insert(0, str(ROOT / "labs" / "_shared"))

from paths import RAW, STAGING, CURATED
from session import get_spark

print("ROOT   ", ROOT)
print("RAW    ", RAW, "existe:", RAW.is_dir())
print("STAGING", STAGING)
print("CURATED", CURATED)
```

Usa `RAW` / `STAGING` / `CURATED`. No escribas `Path("data/raw")`.
