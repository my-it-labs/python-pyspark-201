"""Piezas comunes de los notebooks del curso (tuteo, labs paso a paso)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from nbutil import CELDA_0, code, md, write_notebook  # noqa: E402

NB = ROOT / "notebooks"
TRABAJO = "notebooks/trabajo"

REPO = "https://github.com/my-it-labs/python-pyspark-201"


def fence(src: str) -> str:
    return "```python\n" + src.strip() + "\n```"


def nav(prev: str, nxt: str) -> str:
    return f"[← Anterior]({prev}) · [Siguiente →]({nxt})"


def lab_abre(code_id: str, title: str, filename: str, goal: str, prev: str, nxt: str) -> str:
    return f"""# {code_id} — {title}

{nav(prev, nxt)}

Este fichero es el **guion**. No lo rellenes aquí: **crea tu propio notebook** y ve construyéndolo celda a celda.

## Qué vas a hacer

{goal}

## 0 — Crea tu notebook

1. En el explorador, abre la carpeta `{TRABAJO}/`.
2. Clic derecho → **New File…**
3. Nombre exacto: `{filename}` (incluye `.ipynb`).
4. Ábrelo. Arriba a la derecha (o `F1` → `Notebook: Select Notebook Kernel`) elige **Python (NovaShop)**.
5. Deja **este** guion a un lado (pestaña) y escribe **solo** en el tuyo.

## Cómo organizar *tu* notebook (siempre)

En cada paso creas **dos celdas**, en este orden:

1. **Markdown** — qué vas a hacer y por qué, con tus palabras.
2. **Código** — el de la celda de código del paso. Lo ejecutas (`Shift+Enter`), miras la salida y, si no cuadra, lo mejoras.

No dejes un muro de código sin explicación. Un notebook se lee de arriba abajo, como un cuaderno.

> Kernel **Python (NovaShop)**. Si no aparece: terminal → `bash .devcontainer/setup.sh` → vuelve a elegir kernel.
"""


def paso(
    n: str,
    title: str,
    md_hint: str,
    src: str,
    check: str,
    why: str,
    if_fail: str = "",
    extra: str = "",
) -> list:
    """Instrucciones + celda de código (eso es lo que pegas en el tuyo)."""
    fail = f"\n\n**Si no sale.** {if_fail}" if if_fail else ""
    more = f"\n\n{extra}" if extra else ""
    return [
        md(
            f"""### Paso {n} — {title}

En *tu* notebook: una celda Markdown que explique esto (con tus palabras):

{md_hint}

Debajo, una celda de código. El código está **en la celda siguiente** (márcalo y llévatelo). Ejecuta (`Shift+Enter`).

**Comprueba.** {check}

**Por qué este paso.** {why}{fail}{more}"""
        ),
        code(src.strip()),
    ]


def comprueba(text: str) -> str:
    return f"""## Comprueba

Antes de dar el lab por cerrado, vuelve a ejecutar de arriba abajo (**Run All**) y verifica:

{text}
"""


def prueba(title: str, brief: str, src: str, expect: str) -> list:
    """Experimento: código de partida que hay que alterar, no solo pegar."""
    return [
        md(
            f"""## Prueba tú — {title}

No copies y listo: **cambia** lo que indica el texto y mira si cuadra con **Qué tienes que ver**.

{brief}

**Qué tienes que ver.** {expect}"""
        ),
        code(src.strip()),
    ]


def reto(title: str, brief: str, solucion: str) -> list:
    """Enunciado + solución en celda de código (marcable)."""
    cells = [
        md(
            f"""## Mejora — {title}

{brief}

Si te atasca, el código está en la celda siguiente."""
        )
    ]
    text = solucion.strip()
    if text.startswith("```python"):
        body = text.removeprefix("```python").removesuffix("```").strip()
        cells.append(code(body))
    elif text.startswith("```"):
        body = text.split("\n", 1)[-1]
        if body.endswith("```"):
            body = body[: -3].strip()
        cells.append(code(body))
    else:
        cells.append(code(text))
    return cells


def errores(rows: list[tuple[str, str, str]]) -> str:
    lines = [
        "## Si algo falla",
        "",
        "| Qué ves | Suele ser | Qué haces |",
        "|---------|-----------|-----------|",
    ]
    for a, b, c in rows:
        lines.append(f"| {a} | {b} | {c} |")
    return "\n".join(lines)


def siguiente(path: str, label: str) -> str:
    return f"""## Siguiente

Cuando hayas **comprobado** y (si quieres) **mejorado**, abre [{label}]({path}).
"""


def teoria_head(title: str, intro: str, prev: str, nxt: str) -> str:
    return f"""# {title}

{nav(prev, nxt)}

{intro}

Ejecuta las celdas **aquí**, en este mismo fichero (clase, juntos). Va **montado**: explicación + código + lo que tienes que ver. Lo que construyes tú está en el **lab**.

Kernel: **Python (NovaShop)**.
"""


def boot_cells(app: str) -> list:
    return [
        md(
            """## Arranque

La primera celda **no es Spark todavía**: busca la raíz del repo (aunque este notebook no esté en la carpeta de arriba) y deja `RAW`, `STAGING` y `CURATED` listos. La segunda pide una `SparkSession` en `local[*]` (todos los cores de esta máquina; no hay clúster).

Al ejecutar: rutas impresas y una versión `3.5.x` con master `local[*]`."""
        ),
        code(CELDA_0),
        code(
            f"""# getOrCreate: si ya hay sesión en este kernel, la reusa (mismo puerto 4040)
spark = get_spark('{app}')
print(spark.version, spark.sparkContext.master)"""
        ),
    ]
