"""Labs M04–M07: guion paso a paso (tú creas el notebook)."""
from __future__ import annotations

from .common import CELDA_0, comprueba, errores, lab_abre, md, paso, prueba, reto, siguiente


def m04_01() -> list:
    return [
        md(
            lab_abre(
                "M04-01",
                "Joins",
                "M04-01-joins.ipynb",
                "Medir cuántas líneas y pedidos se pierden al hacer inner contra clientes, y listar los huérfanos.",
                "01-teoria.ipynb",
                "03-lab-kpis.ipynb",
            )
        ),
        *paso(
                "1",
                "Carga fact y clientes",
                "El fact ya trae customer_id de la cabecera. Leo Parquet, no CSV.",
                CELDA_0
                + """

spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
print(fact.count(), customers.count())""",
                "`1980 250`.",
                "Si falta fact_lines, cierra M03-02 primero.",
            ),
        *paso(
                "2",
                "Inner frente a left",
                "La diferencia ES el síntoma de las claves huérfanas. Cuento los dos.",
                """inner = fact.join(customers, "customer_id", "inner")
left = fact.join(customers, "customer_id", "left")
print("inner", inner.count(), "left", left.count())""",
                "inner **1956** · left **1980**.",
                "Si el inner sale mayor que 1980, el join de catálogo te ha duplicado (no lo hagas aquí).",
            ),
        *paso(
                "3",
                "Anti-join de huérfanos",
                "left_anti = está en el fact y no en clientes. Mejor que un where a ciegas.",
                """orphans = fact.join(customers, "customer_id", "left_anti")
orphans.select("order_id", "customer_id").distinct().orderBy("order_id").show()
print("líneas", orphans.count(), "pedidos", orphans.select("order_id").distinct().count())""",
                "**24** líneas · **8** pedidos · `customer_id` tipo `CX*`.",
                "Si no ves CX*, los filtraste en M02-03: regenera staging.",
                extra="Opcional: left a `products_clean` por `product_id`. Las líneas `P999` aparecen con `name` nulo: mismo patrón.",
            ),
        md(
            comprueba(
                "`fact.count() - inner.count()` → **24** líneas (8 pedidos). Anótalo en Markdown."
            )
        ),
        *reto(
                "Mismo patrón a grano pedido",
                "Inner/left de `orders_clean` ⋈ `customers_clean`. Markdown que compare con el grano línea.",
                """```python
orders = spark.read.parquet(str(STAGING / "orders_clean"))
print("orders", orders.count())
print("inner", orders.join(customers, "customer_id", "inner").count())  # 780
print("left ", orders.join(customers, "customer_id", "left").count())   # 788
```""",
            ),
        md(
            errores(
                [
                    ("Inner > 1980", "Productos duplicados en otro join", "`dropDuplicates` en product_id"),
                    ("No veo CX*", "Los filtraste en M02-03", "Regenera staging: solo quitas customer_id vacío"),
                    ("customer_id ambiguo", "Join mal nombrado", "Usa `join(..., \"customer_id\")`"),
                ]
            )
        ),
        md(siguiente("03-lab-kpis.ipynb", "M04-02 KPIs")),
    ]


def m04_02() -> list:
    return [
        md(
            lab_abre(
                "M04-02",
                "KPIs",
                "M04-02-kpis.ipynb",
                "Calcular GMV cobrable, nº de pedidos cobrables, ticket medio y tasa de cancelación sobre el universo **con cliente real**.",
                "02-lab-joins.ipynb",
                "04-lab-segmentacion.ipynb",
            )
        ),
        *paso(
                "1",
                "Universo de venta",
                "KPI de dinero ≠ KPI de operativa. Inner a clientes y solo is_billable para el dinero.",
                CELDA_0
                + """

from pyspark.sql.functions import col

spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
sales = (
    fact.join(customers, "customer_id", "inner")
    .where(col("is_billable"))
)
print(sales.count())""",
                "**1122** líneas cobrables con cliente (1127 − 5 paid huérfanas).",
                "Si usas left, atribuyes GMV a CX*.",
            ),
        *paso(
                "2",
                "Cuatro métricas globales",
                "El ticket medio se calcula a grano pedido: sum(GMV) / countDistinct(order_id), no avg de línea.",
                """from pyspark.sql.functions import sum as fsum, countDistinct, round as fround

kpis = sales.agg(
    fround(fsum("gmv_line"), 2).alias("gmv"),
    countDistinct("order_id").alias("orders"),
)
kpis = kpis.withColumn("aov", fround(col("gmv") / col("orders"), 2))
kpis.show()""",
                "GMV ≈ **400157.73** · pedidos cobrables **469** · AOV ≈ **853**.",
                "Si casteaste a double, el céntimo puede moverse: redondea a 2 decimales.",
            ),
        *paso(
                "3",
                "Tasa de cancelación",
                "El denominador es pedidos (no líneas). Sobre orders_clean inner clientes (780).",
                """from pyspark.sql.functions import avg

orders = spark.read.parquet(str(STAGING / "orders_clean"))
ord_ok = orders.join(customers, "customer_id", "inner")
cancel = ord_ok.agg(
    avg((col("status") == "cancelled").cast("double")).alias("cancel_rate")
)
cancel.show()
print("pedidos con cliente", ord_ok.count())""",
                "≈ **0.22**. Pedidos con cliente **780**.",
                "Si mides sobre `sales` (solo paid), la tasa sale 0.",
            ),
        *paso(
                "4",
                "KPI por canal",
                "channel_norm (no channel) evita partir web/WEB. Ordeno por GMV.",
                """(
    sales.groupBy("channel_norm")
    .agg(
        fround(fsum("gmv_line"), 2).alias("gmv"),
        countDistinct("order_id").alias("orders"),
    )
    .orderBy(col("gmv").desc())
    .show()
)""",
                "Cuatro filas (`app`, `other`, `store`, `web`). `web` o `app` en cabeza.",
                "Este groupBy es el cuadro de mando.",
            ),
        md(
            comprueba(
                """Reproduce `gmv / countDistinct(order_id)` solo con is_billable e inner.
Un número ~850, no ~350 (eso sería media de línea). Escríbelo en Markdown."""
            )
        ),
        *reto(
                "GMV por mes y país",
                "`groupBy(\"order_month\", \"country\")` con la misma regla cobrable. `UNK` aparece si no rellenaste país.",
                """```python
(
    sales.groupBy("order_month", "country")
    .agg(fround(fsum("gmv_line"), 2).alias("gmv"))
    .orderBy("order_month", "country")
    .show(20)
)
```""",
            ),
        md(
            errores(
                [
                    ("GMV ~ 2×", "Join al catálogo duplicado", "`dropDuplicates([\"product_id\"])`"),
                    ("AOV ridículamente bajo", "`avg(\"gmv_line\")`", "`sum / countDistinct(order_id)`"),
                    ("Cancel rate 0", "Mediste sobre sales (solo paid)", "Usa orders_clean"),
                ]
            )
        ),
        md(siguiente("04-lab-segmentacion.ipynb", "M04-03 segmentación")),
    ]


def m04_03() -> list:
    return [
        md(
            lab_abre(
                "M04-03",
                "Segmentación",
                "M04-03-segmentacion.ipynb",
                "Clasificar clientes con venta cobrable en low / mid / high según GMV y contar cada banda.",
                "03-lab-kpis.ipynb",
                "../M05-analisis-avanzado/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "GMV por cliente",
                "La segmentación es una agregación DESPUÉS de fijar el grano. Parto de sales cobrable.",
                CELDA_0
                + """

from pyspark.sql.functions import col, sum as fsum, countDistinct, when, lit

spark = get_spark("novashop-m04")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
sales = fact.join(customers, "customer_id", "inner").where(col("is_billable"))
customer_gmv = sales.groupBy("customer_id", "country", "segment").agg(
    fsum("gmv_line").alias("gmv"),
    countDistinct("order_id").alias("orders"),
)
customer_gmv.orderBy(col("gmv").desc()).show(5)
print(customer_gmv.count())""",
                "~**211** clientes con al menos un paid.",
                "Si agregas *todos* los clientes con left, inflas con GMV nulo.",
            ),
        *paso(
                "2",
                "Bandas de negocio",
                "Umbrales explícitos: <1000 low, <3000 mid, resto high. Encadena when bien (no solapes).",
                """banded = customer_gmv.withColumn(
    "value_band",
    when(col("gmv") < 1000, lit("low"))
    .when(col("gmv") < 3000, lit("mid"))
    .otherwise(lit("high")),
)
banded.groupBy("value_band").count().orderBy("value_band").show()""",
                "`high` ≈ 40 · `low` ≈ 56 · `mid` ≈ 115. Suma = count de customer_gmv.",
                "Los quintiles (`ntile`) van en la mejora, no aquí.",
            ),
        *paso(
                "3",
                "Guarda para M05",
                "M05 rankea sobre este grano sin recalcular el GMV.",
                """banded.write.mode("overwrite").parquet(str(STAGING / "customer_gmv"))
print(spark.read.parquet(str(STAGING / "customer_gmv")).count())""",
                "Carpeta `data/staging/customer_gmv` y el mismo count (~211).",
                "overwrite para poder repetir el lab.",
            ),
        md(
            comprueba(
                "`low + mid + high` debe igualar `customer_gmv.count()`. Una sola cifra, sin clientes en dos bandas."
            )
        ),
        *reto(
                "Quintiles",
                "Usa `ntile(5)` sobre `gmv` (ventana global `orderBy(gmv)`) y cuenta cada quintil. Sin partitionBy: ranking de la compañía.",
                """```python
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

w = Window.orderBy(col("gmv"))
customer_gmv.withColumn("q", ntile(5).over(w)).groupBy("q").count().orderBy("q").show()
```""",
            ),
        md(
            errores(
                [
                    ("250 clientes en las bandas", "Left con GMV nulo", "Parte de sales cobrable"),
                    ("Un cliente en two bands", "Whens solapados", "`< 1000` luego `< 3000` luego otherwise"),
                ]
            )
        ),
        md(siguiente("../M05-analisis-avanzado/01-teoria.ipynb", "M05 — teoría")),
    ]


def m05_01() -> list:
    return [
        md(
            lab_abre(
                "M05-01",
                "Ranking por ventana",
                "M05-01-ranking-ventana.ipynb",
                """La misma ventana de la teoría, a tamaño NovaShop: top 10 clientes por GMV y, **dentro de cada cliente**, sus 3 productos que más dinero dejan.

No hace falta memorizar la API. En cada paso: ejecuta → mira `rn` → **cambia un número o quita un `partitionBy`** y vuelve a ejecutar. Si solo pegas, no has visto la ventana.""",
                "01-teoria.ipynb",
                "03-lab-acumulados.ipynb",
            )
        ),
        *paso(
                "1",
                "Top 10 de la compañía",
                """Parte de `customer_gmv` (M04-03: una fila por cliente con venta cobrable). Sin `partitionBy`, el ranking es **de toda la empresa**: un solo `rn=1`.

`row_number` pone 1 al GMV más alto (`orderBy desc`), 2 al siguiente, etc. El `where rn <= 10` es el top.""",
                CELDA_0
                + """

from pyspark.sql.functions import col, row_number, sum as fsum
from pyspark.sql.window import Window

spark = get_spark("novashop-m05")
# Una fila = un cliente (sale de M04-03). Si PATH falla: rehaz ese lab o run_pipeline no basta
# (customer_gmv lo escribes tú en M04-03).
cust = spark.read.parquet(str(STAGING / "customer_gmv"))
print("clientes con GMV cobrable", cust.count())

# Sin partitionBy: un único ranking para toda la tabla
w_global = Window.orderBy(col("gmv").desc())
top10 = (
    cust.withColumn("rn", row_number().over(w_global))  # 1 = el que más factura
    .where(col("rn") <= 10)
)
top10.orderBy("rn").show()""",
                "10 filas, `rn` de 1 a 10, GMV hacia abajo. El nº 1 ronda **6000 €**.",
                "Si no tienes `customer_gmv`, no es M05: vuelve a M04-03 (groupBy cliente).",
                if_fail="PATH not found → el Parquet vive en `data/staging/customer_gmv` (lo escribes en el lab de segmentación).",
            ),
        *prueba(
                "Cambia el corte del top",
                "En la celda de arriba, cambia `<= 10` por `<= 3` y vuelve a ejecutar. Luego prueba `<= 1`.",
                """print("filas top3", top10.where(col("rn") <= 3).count())  # 3
# ¿El customer_id del rn=1 sigue siendo el mismo que con top 10?
top10.where(col("rn") == 1).select("customer_id", "gmv").show()""",
                "`<= 3` da 3 filas. El nº 1 **no cambia** (solo recortas). Si cambia, reordenaste mal.",
            ),
        *paso(
                "2",
                "Top 3 productos **por** cliente",
                """Ahora el ranking se **reinicia** en cada persona. Eso es `partitionBy("customer_id")`.

Antes hay que **juntar líneas del mismo producto**: si rankeas el fact a palo seco, la misma SKU sale muchas veces (una por línea). Por eso `groupBy(customer_id, product_id)` y luego la ventana.""",
                """fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))

# Dinero cobrable de cada par cliente+producto (ya no es grano línea)
product_gmv = (
    fact.join(customers, "customer_id", "inner")
    .where(col("is_billable"))
    .groupBy("customer_id", "product_id")
    .agg(fsum("gmv_line").alias("gmv"))
)

# El rn vuelve a 1 en CADA customer_id
w_prod = Window.partitionBy("customer_id").orderBy(col("gmv").desc())
top3 = (
    product_gmv.withColumn("rn", row_number().over(w_prod))
    .where(col("rn") <= 3)
)

# Mira solo al cliente que era nº 1 de la compañía
top_id = top10.select("customer_id").first()["customer_id"]
print("cliente nº 1 de la compañía:", top_id)
top3.where(col("customer_id") == top_id).orderBy("rn").show()
print("filas top3 (todos los clientes)", top3.count())""",
                "Como mucho 3 filas por cliente; `rn` 1–3. `filas top3` ≤ 211 × 3. El nº 1 de *ese* cliente es un producto, no el ranking global.",
                "Si ves 30 filas del mismo cliente, rankeaste líneas: faltó el groupBy producto.",
            ),
        *prueba(
                "Quita el partitionBy del top 3",
                "Crea `w_mal = Window.orderBy(col(\"gmv\").desc())` (sin partitionBy), calcula `rn` y filtra `rn <= 3`. Compáralo con `top3`.",
                """w_mal = Window.orderBy(col("gmv").desc())  # ranking de TODA la empresa
mal = product_gmv.withColumn("rn", row_number().over(w_mal)).where(col("rn") <= 3)
print("sin partitionBy, filas", mal.count())  # 3 en total, no 3 por cliente
mal.show()
print("con partitionBy, filas", top3.count())""",
                "Sin `partitionBy`: **3 filas en toda la tabla**. Con él: cientos (3 por cliente). Anota los dos counts en Markdown.",
            ),
        md(
            comprueba(
                """Elige un `customer_id` con varios productos y mira sus `rn`.
Empiezan en **1** (no continúan el 1–10 de la compañía). Markdown: id + tres filas.

También: count sin `partitionBy` vs con él (prueba de arriba)."""
            )
        ),
        *reto(
                "rank vs row_number",
                "Sobre `cust`, añade columnas `row_number`, `rank` y `dense_rank` con el mismo `w_global`. Si hay empate de GMV se ve el salto. Markdown: qué salta y qué no.",
                """```python
from pyspark.sql.functions import rank, dense_rank

cmp_ = (
    cust.withColumn("rn", row_number().over(w_global))
    .withColumn("rk", rank().over(w_global))
    .withColumn("dr", dense_rank().over(w_global))
    .orderBy(col("gmv").desc())
)
cmp_.select("customer_id", "gmv", "rn", "rk", "dr").show(15)
```""",
            ),
        md(
            errores(
                [
                    ("Un solo rn=1 en todo el fact", "Olvidaste partitionBy", "Añádelo para “por cliente”"),
                    ("Top 3 con 30 filas del mismo cliente", "Rankeaste líneas", "groupBy cliente+producto antes"),
                    ("Window sin orderBy", "Ranking indefinido", "Siempre ordena la métrica"),
                    ("No está customer_gmv", "Saltaste M04-03", "Ese lab escribe el Parquet"),
                ]
            )
        ),
        md(siguiente("03-lab-acumulados.ipynb", "M05-02 acumulados")),
    ]


def m05_02() -> list:
    return [
        md(
            lab_abre(
                "M05-02",
                "Acumulados por entidad",
                "M05-02-acumulados.ipynb",
                """Numerar los pedidos de cada cliente (`order_n`) y el GMV cobrable **acumulado** en el tiempo (`gmv_running`).

Misma ventana que la teoría: `partitionBy(cliente)` + `orderBy(fecha)`. Si `gmv_running` baja dentro de un cliente, el orden está mal — no lo copies: **compruébalo**.""",
                "02-lab-ranking-ventana.ipynb",
                "../M06-optimizacion-ejecucion/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "Primero: grano pedido (no línea)",
                """Un pedido con 3 productos no es 3 visitas. Si rankeas o acumulas el fact a palo seco, `order_n` cuenta **líneas**.

Por eso agrupas a `order_id`: fecha del pedido = `min(order_ts)`, dinero = `sum(gmv_line)`.""",
                CELDA_0
                + """

from pyspark.sql.functions import col, min as fmin, sum as fsum, row_number

spark = get_spark("novashop-m05")
orders_gmv = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .join(spark.read.parquet(str(STAGING / "customers_clean")), "customer_id", "inner")
    .where(col("is_billable"))
    .groupBy("customer_id", "order_id")
    .agg(
        fmin("order_ts").alias("order_ts"),  # un instante por ticket
        fsum("gmv_line").alias("gmv"),       # dinero de todas las líneas del ticket
    )
)
print("pedidos cobrables con cliente", orders_gmv.count())""",
                "**469** (el mismo count que el KPI de M04-02). Si salen **1122**, no agregaste a `order_id`: estás en grano línea.",
                "469 tickets ≠ 1122 líneas. El acumulado “por visita” vive en el ticket.",
            ),
        *prueba(
                "¿Qué pasa si no agrupas?",
                "Cuenta el fact cobrable+inner **sin** el `groupBy` de `order_id`. Compáralo con 469.",
                """lineas = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .join(spark.read.parquet(str(STAGING / "customers_clean")), "customer_id", "inner")
    .where(col("is_billable"))
)
print("líneas", lineas.count(), "pedidos distintos", lineas.select("order_id").distinct().count())""",
                "`líneas` **1122**, `pedidos distintos` **469**. Si usas 1122 como “nº de pedido”, estás inflando visitas.",
            ),
        *paso(
                "2",
                "Número de pedido y acumulado",
                """Una sola window para las dos columnas: el vecindario es el cliente; el eje es el tiempo.

`row_number` → 1.er, 2.º, 3.er ticket de **esa** persona.
`sum(gmv).over(w)` → dinero desde el primer ticket **hasta este** (incluido).""",
                """from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_ts")
hist = (
    orders_gmv.withColumn("order_n", row_number().over(w))
    .withColumn("gmv_running", fsum("gmv").over(w))
)
hist.orderBy("customer_id", "order_n").show(12)""",
                "`order_n` 1, 2, 3… **por cliente**. `gmv_running` no decrece dentro del mismo `customer_id`.",
                "Si baja, el `orderBy` de la window no es `order_ts` (o está descendente).",
            ),
        *prueba(
                "Un cliente concreto",
                "Elige un `customer_id` que en el `show` tenga `order_n` ≥ 2. Filtra solo ese id y mira si la fila 2 tiene `gmv_running` ≥ fila 1. Anota id y las dos filas en Markdown.",
                """# Cambia el id por uno que hayas visto con varios pedidos
cid = hist.where(col("order_n") >= 2).select("customer_id").first()["customer_id"]
print("cliente", cid)
hist.where(col("customer_id") == cid).orderBy("order_n").show()""",
                "Al menos dos filas. `gmv_running` de `order_n=2` ≥ el de `order_n=1`. Si no, el orden de la ventana está al revés.",
            ),
        *paso(
                "3",
                "Primera compra vs repetición",
                """`order_n == 1` es la definición de “nuevo” en este curso: primer ticket cobrable de ese cliente. El resto son repeticiones.""",
                """hist.groupBy((col("order_n") == 1).alias("is_first")).count().show()""",
                "~211 primeras compras (`true`: un cliente con paid) y el resto `false` (repeticiones). 211 + repeticiones = 469.",
                "Sin `partitionBy`, `order_n=1` sería **una sola fila en toda la empresa**.",
            ),
        *prueba(
                "Ventana de toda la empresa",
                "Repite el paso 2 con `w_emp = Window.orderBy(\"order_ts\")` (sin partitionBy). Cuenta cuántos `order_n == 1` hay.",
                """w_emp = Window.orderBy("order_ts")  # un solo ranking temporal global
hist_emp = orders_gmv.withColumn("order_n", row_number().over(w_emp))
print("order_n=1 sin partitionBy", hist_emp.where(col("order_n") == 1).count())  # 1
print("order_n=1 con partitionBy", hist.where(col("order_n") == 1).count())     # ~211""",
                "Sin `partitionBy`: **1**. Con él: ~**211**. Esa diferencia *es* la ventana.",
            ),
        md(
            comprueba(
                """Un cliente con `order_n` ≥ 2: `gmv_running` fila 2 ≥ fila 1. Markdown con el id.

Counts: 469 pedidos; ~211 primeros; `order_n=1` global (sin partitionBy) = 1."""
            )
        ),
        *reto(
                "Pedidos hasta superar 1000 €",
                "Quédate, por cliente, con la **primera** fila donde `gmv_running >= 1000` (o ninguna si no llega). Markdown: ¿`order_n` 1 o hace falta el 2.º ticket?",
                """```python
w2 = Window.partitionBy("customer_id").orderBy("order_ts")
crossed = hist.where(col("gmv_running") >= 1000)
first_cross = crossed.withColumn("rn", row_number().over(w2)).where(col("rn") == 1)
first_cross.select("customer_id", "order_n", "gmv_running").show()
print("clientes que cruzan 1000", first_cross.count())
```""",
            ),
        md(
            errores(
                [
                    ("gmv_running igual en todas las filas", "Window sin orderBy", "partitionBy + orderBy(order_ts)"),
                    ("1122 “pedidos”", "No agregaste a order_id", "Paso 1"),
                    ("Acumulado a nivel empresa", "Falta partitionBy", "Añádelo"),
                    ("order_n=1 solo una vez", "Ventana global", "partitionBy(customer_id)"),
                ]
            )
        ),
        md(siguiente("../M06-optimizacion-ejecucion/01-teoria.ipynb", "M06 — teoría")),
    ]

def m06_01() -> list:
    return [
        md(
            lab_abre(
                "M06-01",
                "Explain y DAG",
                "M06-01-explain-dag.ipynb",
                "Demostrar que cinco transformaciones no lanzan job, y señalar scan + filtro en el plan formateado.",
                "01-teoria.ipynb",
                "03-lab-cache-particionado.ipynb",
            )
        ),
        md(
            """## Spark UI

Abre la pestaña **Ports** del Codespace → puerto **4040**. Anota el último Job Id que ves *antes* del paso 2 (puede ser 0).
"""
        ),
        *paso(
                "1",
                "Plan sin ejecutar",
                "Imprimir el objeto DataFrame no dispara jobs. Encadeno wheres y un select.",
                CELDA_0
                + """

from pyspark.sql.functions import col

spark = get_spark("novashop-m06")
planned = (
    spark.read.parquet(str(STAGING / "fact_lines"))
    .where(col("is_billable"))
    .where(col("gmv_line") > 0)
    .where(col("channel_norm").isin("web", "app"))
    .select("order_id", "customer_id", "gmv_line", "order_month")
)
print(planned)""",
                "El Job Id **más alto** de Spark UI **no cambia** al ejecutar esta celda.",
                "Lazy de verdad: no hay acción.",
            ),
        *paso(
                "2",
                "Una acción, un DAG",
                "count obliga a recorrer las particiones. Miro UI: un job nuevo.",
                """print(planned.count())""",
                "Un job nuevo. En Jobs, el DAG muestra al menos un stage. El count es líneas cobrables web/app con GMV > 0 (varios cientos).",
                "Si cada celda crea un job, tienes un `.show()` de debug por medio.",
            ),
        *paso(
                "3",
                "Lee el plan",
                "Busco FileScan parquet (o Scan) y Filter. No traduzco cada operador Catalyst.",
                """planned.explain("formatted")""",
                "Aparece el path `fact_lines` y predicados `is_billable` / `gmv_line` / `channel_norm`.",
                "Copia en Markdown las dos líneas que identifican scan y filtro.",
            ),
        md(
            comprueba(
                """Encadena un `.where(...)` extra **sin** `count` y mira Jobs.
No hay job nuevo. Escríbelo."""
            )
        ),
        *reto(
                "explain(True) vs formatted",
                "Compara `explain(True)` con `explain(\"formatted\")`. ¿Dónde se ve el filtro empujado al scan?",
                "En el físico / formatted, `PushedFilters` o el Filter junto al FileScan indica predicate pushdown. Si el filtro no aparece, lo aplicaste *después* de un select que ya tiró la columna.",
            ),
        md(
            errores(
                [
                    ("UI vacía / 404", "Puerto 4040 no reenviado", "Ports del Codespace → 4040"),
                    ("Cada celda crea un job", "Tienes un show de debug", "Coméntalo mientras mides"),
                    ("Dos sesiones", "SparkSession() extra", "Solo get_spark()"),
                ]
            )
        ),
        md(siguiente("03-lab-cache-particionado.ipynb", "M06-02 cache")),
    ]


def m06_02() -> list:
    return [
        md(
            lab_abre(
                "M06-02",
                "Cache y particionado",
                "M06-02-cache-particionado.ipynb",
                "Materializar un cache con una acción y ver cómo `repartition(\"order_month\")` cambia el número de particiones.",
                "02-lab-explain-dag.ipynb",
                "../M07-persistencia-datos/01-teoria.ipynb",
            )
        ),
        *paso(
                "1",
                "Un fact más largo",
                "8 copias: suficiente para notar el cache en local, sin saturar el Codespace.",
                CELDA_0
                + """

from pyspark.sql.functions import col, lit

spark = get_spark("novashop-m06")
base = spark.read.parquet(str(STAGING / "fact_lines"))
xl = base.withColumn("_copy", lit(-1))
for i in range(7):
    xl = xl.unionByName(base.withColumn("_copy", lit(i)))
print("particiones iniciales", xl.rdd.getNumPartitions())""",
                "Varias particiones (> 1). Count esperado **15840** cuando lo lances.",
                "1980 × 8. Poco para un clúster; bastante para ver Storage en local.",
            ),
        *paso(
                "2",
                "Dos counts sin cache",
                "Cada acción relee el plan desde el Parquet + unions. Dos jobs de coste parecido.",
                """import time

def timed_count(df, label):
    t0 = time.perf_counter()
    n = df.where(col("is_billable")).count()
    print(label, n, f"{time.perf_counter() - t0:.2f}s")

timed_count(xl, "1er count frío")
timed_count(xl, "2º count frío")""",
                "Dos tiempos del mismo orden. Cobrable = 1127 × 8 = **9016**.",
                "En local a veces es tan corto que el cronómetro no emociona: mira Jobs.",
            ),
        *paso(
                "3",
                "Cache materializado",
                "cache() no llena Storage hasta una acción. Primero calientas; después lees memoria.",
                """warm = xl.where(col("is_billable")).cache()
timed_count(warm, "calentamiento (materializa cache)")
timed_count(warm, "caliente")""",
                "En Spark UI → Storage aparece el DataFrame. El segundo tiempo **no empeora**.",
                "La primera acción llena Storage; la segunda debería leer memoria.",
                extra="Al terminar: `warm.unpersist()` (otra celda, con su Markdown).",
            ),
        *paso(
                "4",
                "Repartition por mes",
                "repartition(12, order_month) hace shuffle hacia 12 particiones. Es preparación para escribir (M07), no una window.",
                """by_month = warm.repartition(12, col("order_month"))
print("particiones", by_month.rdd.getNumPartitions())
by_month.groupBy("order_month").count().orderBy("order_month").show()""",
                "`particiones 12`. Doce meses en el groupBy.",
                "Si haces `repartition(col)` sin `n`, en 3.5 usas 200 particiones por defecto.",
            ),
        md(
            comprueba(
                """Ejecuta solo `.cache()` y mira Storage **antes** de cualquier count → vacío.
Luego un count → aparece. Escríbelo."""
            )
        ),
        *reto(
                "coalesce vs repartition",
                "Pasa a 1 partición con `coalesce(1)` y con `repartition(1)`. ¿Cuál declara shuffle en el plan?",
                "`repartition(1)` siempre shufflea. `coalesce(1)` reduce sin shuffle amplio. Útil para un único fichero de entrega; malo como hábito de pipeline (M07).",
            ),
        md(
            errores(
                [
                    ("Storage vacío tras cache()", "No hubo acción", "count() o show()"),
                    ("OOM", "Demasiadas copias + cache", "Quédate en 8; unpersist"),
                    ("1980 o 200 particiones", "repartition sin n", "repartition(12, col(\"order_month\"))"),
                ]
            )
        ),
        md(siguiente("../M07-persistencia-datos/01-teoria.ipynb", "M07 — teoría")),
    ]


def m07_01() -> list:
    return [
        md(
            lab_abre(
                "M07-01",
                "Parquet y layout analítico",
                "M07-01-parquet-layout.ipynb",
                "Publicar `data/curated/sales_analytics` en Parquet particionado por mes y demostrar que un filtro de mes no lee el año entero.",
                "01-teoria.ipynb",
                "../../README.md",
            )
        ),
        *paso(
                "1",
                "Dataset curated",
                "Left al catálogo conserva P999. Inner a clientes quita CX*. Solo paid. dropDuplicates en product_id.",
                CELDA_0
                + """

from pyspark.sql.functions import col

spark = get_spark("novashop-m07")
fact = spark.read.parquet(str(STAGING / "fact_lines"))
customers = spark.read.parquet(str(STAGING / "customers_clean"))
products = (
    spark.read.parquet(str(STAGING / "products_clean"))
    .dropDuplicates(["product_id"])
)
sales = (
    fact.join(customers, "customer_id", "inner")
    .join(products, "product_id", "left")
    .where(col("is_billable"))
    .select(
        "order_id", "order_ts", "order_month",
        "customer_id", "country", "segment",
        "product_id", "category",
        "qty", "unit_price", "discount", "gmv_line", "channel_norm",
    )
)
print(sales.count())""",
                "**1122** filas (mismo universo que M04-02).",
                "Si sale 2244, el catálogo no era único.",
            ),
        *paso(
                "2",
                "Escribe Parquet por mes",
                "overwrite deja el curated idempotente. Doce particiones = doce meses de 2024.",
                """dest = CURATED / "sales_analytics"
CURATED.mkdir(parents=True, exist_ok=True)
(
    sales.write.mode("overwrite")
    .partitionBy("order_month")
    .parquet(str(dest))
)
print(sorted(p.name for p in dest.iterdir() if p.is_dir()))""",
                "Carpetas `order_month=2024-01` … `order_month=2024-12` (más `_SUCCESS`).",
                "Esto es layout de disco, no el repartition de M06.",
            ),
        *paso(
                "3",
                "Prune al leer un mes",
                "El plan debe listar solo marzo (o PartitionFilters: order_month=2024-03).",
                """marzo = spark.read.parquet(str(dest)).where(col("order_month") == "2024-03")
marzo.explain("formatted")
print("marzo", marzo.count(), "total", spark.read.parquet(str(dest)).count())""",
                "Total **1122**. `marzo` es un subconjunto. El formatted menciona `2024-03`.",
                "Copia en Markdown la línea del PartitionFilters.",
            ),
        *paso(
                "4",
                "CSV vs Parquet (schema, no solo tamaño)",
                "coalesce(1) solo existe aquí para comparar *un* CSV, no como patrón. Releo los dos schemas.",
                """import os

csv_dir = CURATED / "_csv_compare"
sales.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_dir))

def du(path):
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

print("parquet", du(dest), "csv", du(csv_dir))
spark.read.parquet(str(dest)).printSchema()
spark.read.option("header", True).csv(str(csv_dir)).printSchema()""",
                "Parquet mantiene `decimal`/`timestamp`. El CSV vuelve a string. El tamaño: Parquet suele ganar; en este volumen a veces es parecido.",
                "Curated en CSV “para el analista” pierde tipos.",
            ),
        md(
            comprueba(
                """Vuelve a ejecutar el `write.mode(\"overwrite\")` y cuenta.
Sigue **1122**. No se duplica. Anótalo."""
            )
        ),
        *reto(
                "Dos claves de partición",
                "Copia `sales_analytics_geo` con `partitionBy(\"order_month\", \"country\")` y lee marzo ∧ ES. No particiones por customer_id.",
                """```python
geo = CURATED / "sales_analytics_geo"
sales.write.mode("overwrite").partitionBy("order_month", "country").parquet(str(geo))
(
    spark.read.parquet(str(geo))
    .where((col("order_month") == "2024-03") & (col("country") == "ES"))
    .explain("formatted")
)
```""",
            ),
        md(
            errores(
                [
                    ("Miles de part-000xx", "repartition(200) residual", "repartition(12, order_month) antes del write"),
                    ("Count 2244", "append o join duplicado", "overwrite + dropDuplicates de productos"),
                    ("order_month no está al leer", "API antigua", "spark.read.parquet de 3.5 sí la incluye"),
                ]
            )
        ),
        md(siguiente("../../README.md", "índice del curso")),
    ]
