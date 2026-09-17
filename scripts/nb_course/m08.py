"""M08 extra — JSON anidado, roundtrip y schema legacy (teoría montada + lab)."""
from __future__ import annotations

from .common import (
    CELDA_0,
    comprueba,
    errores,
    lab_abre,
    md,
    paso,
    prueba,
    reto,
    siguiente,
    teoria_head,
    code,
    boot_cells,
)


def teoria() -> list:
    return [
        md(
            teoria_head(
                "M08 — JSON anidado y schema que cambia (extra)",
                """**Extra.** El pipeline de pedidos (CSV → fact → Parquet) **no** pasa por aquí. Esto cubre lo que M02 no enseña: un JSON de **varios niveles**, bajarlo a columnas, enriquecerlo y **volver a un documento** que una app (o `mongoimport`) pueda comer. Y el otro dolor: el CRM de 2023 y el de 2024 **no tienen las mismas claves**.

En clase ejecutamos **este** fichero, de arriba abajo. El lab es donde construyes tú el mismo flujo sobre los dumps reales.""",
                "../M02-ingesta-preparacion/04-lab-calidad-limpieza.ipynb",
                "02-lab-json-anidado-schema.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m08"),
        md(
            """## Qué hay en `data/raw/` (además de lo de siempre)

NovaShop “tuvo un CRM”:

| Fichero | Qué es | Filas |
|---------|--------|------:|
| `profiles_v1.jsonl` | Dump **2023**, plano (`fullName`, `country`, `email` string) | **100** (C0001–C0100) |
| `profiles_v2.jsonl` | Dump **2024**, anidado (`profile.contact.address.geo`…) | **200** (C0051–C0250) |

**50** clientes están en los dos (C0051–C0100): la migración se quedó a medias. v2 trae suciedad: 8 sin `address.country` (el país está en `profile.country`), 6 con `orders_preview` vacío, 5 sin email de trabajo.

`customers.csv` sigue siendo la ficha del pipeline. Estos JSON son **otra** fuente."""
        ),
        md(
            """## Un documento v2 (para no perderse en el schema)

Esto es **un** cliente. Spark, al leer el JSONL, convierte cada llave anidada en `struct` y cada lista en `array`.

```json
{
  "customer_id": "C0051",
  "profile": {
    "contact": {
      "full_name": "Cliente 0051",
      "email": {"work": "c0051@novashop.test", "personal": null},
      "address": {
        "city": "Madrid",
        "country": "ES",
        "geo": {"lat": 40.42, "lon": -3.7}
      }
    },
    "prefs": {"channel": "web", "lang": "es"},
    "country": null
  },
  "orders_preview": [{"id": "X00510", "gmv": 12.5}],
  "meta": {"source": {"system": "crm", "version": 2}}
}
```

Niveles: documento → `profile` → `contact` → `address` → `geo`. Eso es lo que pedían “bastantes niveles”. `products.json` del M02 no llega ni a uno."""
        ),
        md(
            """## Leer v2 e **inferir**: el schema *es* el árbol

JSONL: una línea = un objeto. **No** uses `multiLine` (eso era el array de productos).

Al ejecutar: `count` **200**. `printSchema()` enseña `struct` y `array`. Si ves todo `string` y ningún `struct`, no es este fichero."""
        ),
        code(
            """v2 = spark.read.json(str(RAW / "profiles_v2.jsonl"))
print("v2 filas", v2.count())
v2.printSchema()
v2.select("customer_id", "profile.contact.full_name", "profile.contact.address.geo.lat").show(3, truncate=False)"""
        ),
        md(
            """## Bajar un nivel: el punto (`a.b.c`) no explota filas

`col("profile.contact.address.country")` es **una columna**. Sigue habiendo 200 filas. El árbol no se copia a 200 × N productos.

Al ejecutar: 200 filas; algunos `country` nulos (los 8 sucios). `email.work` nulo en 5."""
        ),
        code(
            """from pyspark.sql.functions import col, coalesce, lit, size, explode, struct, to_json, when, row_number
from pyspark.sql.window import Window

print("filas", v2.count())
print(
    "address.country nulo",
    v2.where(col("profile.contact.address.country").isNull()).count(),
)  # 8
print(
    "email.work nulo",
    v2.where(col("profile.contact.email.work").isNull()).count(),
)  # 5
v2.select(
    "customer_id",
    col("profile.contact.address.country").alias("addr_country"),
    col("profile.country").alias("profile_country"),
).where(col("profile.contact.address.country").isNull()).show()"""
        ),
        md(
            """## `explode`: aquí **sí** cambian las filas

`orders_preview` es un array. `explode` convierte **cada elemento en una fila**. Un cliente con 3 previews pasa a 3 filas. Uno con lista vacía **desaparece** (`explode`); `explode_outer` lo deja con nulos.

Al ejecutar: más de 200 filas (casi todos tienen 1–3 previews; 6 tienen 0 y se caen con `explode`)."""
        ),
        code(
            """prev = v2.select("customer_id", explode("orders_preview").alias("item"))
print("filas tras explode", prev.count())  # > 200
prev.select("customer_id", "item.id", "item.gmv").show(6, truncate=False)
print("clientes que se cayeron (preview vacío)", v2.count() - prev.select("customer_id").distinct().count())  # 6"""
        ),
        md(
            """## Contrato interno: una fila por cliente, columnas planas

Para *transformar* (nombres, país, recuentos) conviene **aplanar**. El país se rescata con el mismo truco que las fechas de M02: `coalesce` de dos sitios + `UNK`.

Al ejecutar: 200 filas; `country` nulo **0** (los 8 sucios salen del `profile.country`). `n_preview` 0 en 6 clientes."""
        ),
        code(
            """v2_flat = v2.select(
    col("customer_id"),
    col("profile.contact.full_name").alias("full_name"),
    coalesce(
        col("profile.contact.address.country"),
        col("profile.country"),
        lit("UNK"),
    ).alias("country"),
    col("profile.contact.email.work").alias("email_work"),
    col("profile.contact.address.city").alias("city"),
    size(col("orders_preview")).alias("n_preview"),
    lit("v2").alias("feed"),
)
print("country nulo", v2_flat.where(col("country").isNull()).count())  # 0
print("n_preview=0", v2_flat.where(col("n_preview") == 0).count())  # 6
v2_flat.show(5, truncate=False)"""
        ),
        md(
            """## Subir otra vez: `struct` + JSON (lo que “come” una app / Mongo)

La aplicación no quiere 8 columnas CSV. Quiere **el árbol**. Montas `struct(...)` y, si hace falta un string, `to_json`. Escribir JSONL es `write.json` (un objeto por fichero-partición; Spark deja un directorio).

Esto **no** es un connector Mongo. Es el documento enriquecido. Un `mongoimport` o un POST a un API usarían ese JSON.

Al ejecutar: una columna `doc` con llaves anidadas; el `show` recorta el string."""
        ),
        code(
            """nested = v2_flat.select(
    "customer_id",
    struct(
        struct(
            col("full_name"),
            struct(col("email_work").alias("work")).alias("email"),
            struct(col("city"), col("country")).alias("address"),
        ).alias("contact"),
        struct(col("n_preview").alias("preview_orders")).alias("stats"),
    ).alias("profile"),
    lit("novashop.profile.v2").alias("schema_id"),
)
nested.printSchema()
nested.select("customer_id", to_json(col("profile")).alias("profile_json")).show(2, truncate=80)

from paths import ensure_dirs

ensure_dirs()
dest = CURATED / "_demo_m08_profiles"
nested.write.mode("overwrite").json(str(dest))
print("escrito", dest)
print("releer", spark.read.json(str(dest)).count())  # 200"""
        ),
        md(
            """## Legacy: v1 no tiene `profile`

Mismo negocio, **otro contrato**. `fullName` vs `full_name`. `email` string vs `email.work`. Si haces `v1.union(v2)` a palo seco, Spark exige las mismas columnas en el mismo orden → peta o rellena basura.

Al ejecutar: v1 **100** filas, schema **plano** (todo al primer nivel)."""
        ),
        code(
            """v1 = spark.read.json(str(RAW / "profiles_v1.jsonl"))
print("v1 filas", v1.count())
v1.printSchema()
v1.show(3, truncate=False)"""
        ),
        md(
            """## Unir las dos épocas: normalizas **cada** feed al mismo contrato

1. Aplanas v1 con los nombres **internos** (`full_name`, `country` vacío → nulo).
2. `unionByName(..., allowMissingColumns=True)` — las columnas que falten se crean nulas.
3. En el solape (50 ids) **gana v2** (`row_number` por `customer_id`, `feed` desc).

Al ejecutar: unión bruta 300 filas; después del “quédate con una ficha por id”: **250**. Eso son todos los clientes de NovaShop. Sin esto, o pierdes a C0001–C0050 (solo v1) o duplicas a C0051–C0100."""
        ),
        code(
            """v1_flat = v1.select(
    col("customer_id"),
    col("fullName").alias("full_name"),
    when(col("country") == "", None).otherwise(col("country")).alias("country"),
    col("email").alias("email_work"),
    lit(None).cast("string").alias("city"),
    lit(0).alias("n_preview"),
    lit("v1").alias("feed"),
)
bruto = v2_flat.unionByName(v1_flat)
print("unión bruta (con duplicados de solape)", bruto.count())  # 300
w = Window.partitionBy("customer_id").orderBy(col("feed").desc())  # v2 antes que v1
profiles = (
    bruto.withColumn("rn", row_number().over(w))
    .where(col("rn") == 1)
    .drop("rn")
)
print("una ficha por cliente", profiles.count())  # 250
print("vienen de v1", profiles.where(col("feed") == "v1").count())  # 50  (C0001–C0050)
print("vienen de v2", profiles.where(col("feed") == "v2").count())  # 200"""
        ),
        md(
            """Eso es “resolver el legacy cuando la migración no se hizo bien”: **un contrato interno**, `coalesce` de sitios distintos, y una regla de precedencia (aquí: el dump nuevo pisa al viejo). Spark no “adivina” el CRM; tú fijas qué gana.

**Siguiente:** [lab](02-lab-json-anidado-schema.ipynb) — creas el notebook y repites el flujo (con pruebas). El pipeline de pedidos no cambia."""
        ),
    ]


def lab() -> list:
    return [
        md(
            lab_abre(
                "M08-01",
                "JSON anidado y schema legacy (extra)",
                "M08-01-json-anidado-schema.ipynb",
                """**Extra.** No bloquea M03. Lees los dumps CRM (`profiles_v1` / `v2`), aplanas, unes las dos épocas y escribes un JSON anidado de salida.

Hazlo **después** de M02-02 (ya sabes schema y `coalesce`). Si no has generado datos: `python3 scripts/generate_novashop.py`.""",
                "01-teoria.ipynb",
                "../M03-transformacion-datos/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "Arranque y los dos dumps",
                "Celda 0 + sesión. Cuento v1 y v2 **antes** de cruzarlos. Si v2 no es 200, el generador es viejo.",
                CELDA_0
                + """

spark = get_spark("novashop-m08")
v1 = spark.read.json(str(RAW / "profiles_v1.jsonl"))
v2 = spark.read.json(str(RAW / "profiles_v2.jsonl"))
print("v1", v1.count(), "v2", v2.count())
v2.printSchema()""",
                "`v1 100` · `v2 200`. Schema de v2 con `struct` (`profile`, `contact`, `address`, `geo`) y `array` (`orders_preview`).",
                "v1 es plano (M02). v2 es el árbol. No uses `multiLine`: son JSONL.",
                if_fail="PATH / count 0: `python3 scripts/generate_novashop.py` y vuelve a la celda.",
            ),
        *paso(
                "2",
                "Bajar el árbol a columnas",
                "`coalesce` del país (address vs profile vs UNK), igual que las dos fechas de M02. `size` del array no explota filas.",
                """from pyspark.sql.functions import (
    col, coalesce, lit, size, explode, struct, to_json, when, row_number,
)
from pyspark.sql.window import Window

v2_flat = v2.select(
    col("customer_id"),
    col("profile.contact.full_name").alias("full_name"),
    coalesce(
        col("profile.contact.address.country"),
        col("profile.country"),
        lit("UNK"),
    ).alias("country"),
    col("profile.contact.email.work").alias("email_work"),
    col("profile.contact.address.city").alias("city"),
    size(col("orders_preview")).alias("n_preview"),
    lit("v2").alias("feed"),
)
print("country nulo", v2_flat.where(col("country").isNull()).count())
print("n_preview=0", v2_flat.where(col("n_preview") == 0).count())
v2_flat.show(3, truncate=False)""",
                "`country` nulo **0**. `n_preview=0` **6**. 200 filas.",
                "Si `country` nulo = 8, no pusiste el `coalesce` de `profile.country`.",
            ),
        *prueba(
                "explode vs size",
                "Cuenta filas con `explode(\"orders_preview\")` y compáralo con `v2.count()`. Luego prueba `explode_outer`.",
                """from pyspark.sql.functions import explode_outer

inner_e = v2.select("customer_id", explode("orders_preview").alias("item"))
outer_e = v2.select("customer_id", explode_outer("orders_preview").alias("item"))
print("explode", inner_e.count(), "distinct clientes", inner_e.select("customer_id").distinct().count())
print("explode_outer", outer_e.count(), "distinct", outer_e.select("customer_id").distinct().count())""",
                "`explode`: distinct clientes **194** (se caen 6). `explode_outer`: distinct **200**. El `size` del paso 2 no cambia el count de clientes.",
            ),
        *paso(
                "3",
                "Aplanar v1 al **mismo** contrato",
                "`fullName` → `full_name`. País `\"\"` → nulo. `city` y `n_preview` no existen en 2023: nulo y 0. Columna `feed=v1`.",
                """v1_flat = v1.select(
    col("customer_id"),
    col("fullName").alias("full_name"),
    when(col("country") == "", None).otherwise(col("country")).alias("country"),
    col("email").alias("email_work"),
    lit(None).cast("string").alias("city"),
    lit(0).alias("n_preview"),
    lit("v1").alias("feed"),
)
v1_flat.printSchema()
print(v1_flat.count())""",
                "100 filas. Mismos nombres de columna que `v2_flat` (si no, el union chilla).",
                "Sin `cast` en `city`, el union puede fallar por tipos.",
            ),
        *paso(
                "4",
                "Unir épocas y quedarte con una ficha",
                "`unionByName` respeta nombres, no el orden. En el solape (50 ids) gana **v2**.",
                """bruto = v2_flat.unionByName(v1_flat)
print("bruto", bruto.count())  # 300
w = Window.partitionBy("customer_id").orderBy(col("feed").desc())
profiles = (
    bruto.withColumn("rn", row_number().over(w))
    .where(col("rn") == 1)
    .drop("rn")
)
print("únicos", profiles.count())  # 250
print("feed v1", profiles.where(col("feed") == "v1").count())  # 50
print("feed v2", profiles.where(col("feed") == "v2").count())  # 200""",
                "**300** → **250**. 50 fichas solo-v1, 200 de v2 (el solape se queda con el árbol 2024).",
                "Si usas `union` clásico y el orden de columnas no coincide, el `email` se mete en `city`.",
            ),
        *prueba(
                "¿Y si gana v1?",
                "Cambia el `orderBy` a `col(\"feed\").asc()` (v1 primero). Cuenta `feed==v2` otra vez. Luego **deja otra vez desc** (v2 gana): esa es la regla de negocio del curso.",
                """w_v1 = Window.partitionBy("customer_id").orderBy(col("feed").asc())
alt = bruto.withColumn("rn", row_number().over(w_v1)).where(col("rn") == 1)
print("si gana v1, filas v2", alt.where(col("feed") == "v2").count())  # 150 (C0101–C0250)
print("regla del curso (gana v2)", profiles.where(col("feed") == "v2").count())  # 200""",
                "Con `asc`: v2 baja a **150**. Markdown: qué clientes perderían el árbol anidado (C0051–C0100).",
            ),
        *paso(
                "5",
                "Escribir el documento enriquecido",
                "Parquet plano en staging (para Spark). JSON anidado en curated (para la app / un import a Mongo). `overwrite` para poder re-ejecutar.",
                """from paths import ensure_dirs

ensure_dirs()
profiles.write.mode("overwrite").parquet(str(STAGING / "profiles_flat"))

nested = profiles.select(
    "customer_id",
    struct(
        struct(
            col("full_name"),
            struct(col("email_work").alias("work")).alias("email"),
            struct(col("city"), col("country")).alias("address"),
        ).alias("contact"),
        struct(col("n_preview").alias("preview_orders")).alias("stats"),
    ).alias("profile"),
    col("feed"),
    lit("novashop.profile.v2").alias("schema_id"),
)
out = CURATED / "profiles_nested"
nested.write.mode("overwrite").json(str(out))
re = spark.read.json(str(out))
print("parquet", spark.read.parquet(str(STAGING / "profiles_flat")).count())
print("json", re.count())
re.printSchema()
re.select("customer_id", "profile.contact.address.country").show(3)""",
                "`parquet` y `json` **250**. El schema releído vuelve a tener `struct`. Una app leería esos JSON; Mongo, un `mongoimport` del directorio.",
                "Spark escribe un **directorio** de JSON, no un único `.json` bonito. Es normal.",
            ),
        md(
            comprueba(
                """v1=100, v2=200, únicos=250. `country` nulo 0 tras coalesce.
JSON relído: 250 y `profile.contact` existe. Markdown: por qué 300 ≠ 250."""
            )
        ),
        *reto(
                "Clientes UNK",
                "Cuenta `country == \"UNK\"` en `profiles`. ¿Vienen de v1, de v2 o de los dos? Markdown con el `feed`.",
                """```python
unk = profiles.where(col("country") == "UNK")
print("UNK", unk.count())
unk.groupBy("feed").count().show()
```""",
            ),
        md(
            errores(
                [
                    ("v2 count ≠ 200", "Generador antiguo", "`python3 scripts/generate_novashop.py`"),
                    ("union AnalysisException", "Nombres/tipos distintos", "Aplana v1 al mismo contrato; `city` con `cast`"),
                    ("250 no sale", "No filtraste rn==1", "Window por customer_id"),
                    ("explode = 200", "Array vacío o no explotaste", "Casi todos tienen 1–3 items; tiene que subir"),
                    ("Un único .json", "Spark escribe carpeta", "Lee con `spark.read.json(dir)`"),
                ]
            )
        ),
        md(siguiente("../M03-transformacion-datos/01-teoria.ipynb", "M03 — o sigue el extra y vuelve al pipeline")),
    ]
