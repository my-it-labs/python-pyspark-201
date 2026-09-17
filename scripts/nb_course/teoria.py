"""Teoría M01–M07: explicación y demo intercaladas (tú ejecutas aquí)."""
from __future__ import annotations

from .common import boot_cells, code, md, teoria_head


def m01() -> list:
    return [
        md(
            teoria_head(
                "M01 — Fundamentos y entorno",
                """Si vienes de Pandas, este notebook es el puente. Vamos a coger **las mismas cinco filas** y tratarlas primero como lo harías en un script de analista (Pandas) y después como lo hace Spark.

No hace falta que memorices la API. Fíjate en *cuándo* aparece el resultado: en Pandas, en la línea de debajo; en Spark, solo cuando pides una acción (`show`, `count`).

Después creas el lab en `notebooks/trabajo/`. Guion: `02-lab-sesion-spark.ipynb`.""",
                "../M00-entorno-notebooks/02-lab-primer-notebook.ipynb",
                "02-lab-sesion-spark.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m01"),
        md(
            """## Un dataset pequeño en Pandas

Vamos a inventar cinco pedidos. En Pandas, `DataFrame(...)` **construye la tabla en ese momento**: las filas ya están en la RAM de este proceso Python. Al ejecutar la celda verás la tabla con una columna extra a la izquierda (`0, 1, 2…`): es el **índice**. Spark no tiene índice de filas; es la primera diferencia que vas a notar."""
        ),
        code(
            """import pandas as pd

# Cinco pedidos de juguete. Cada dict es una fila.
pedidos_pd = pd.DataFrame(
    [
        {"order_id": "O1", "status": "paid", "amount": 10.0},
        {"order_id": "O2", "status": "cancelled", "amount": 20.0},
        {"order_id": "O3", "status": "paid", "amount": 5.0},
        {"order_id": "O4", "status": "paid", "amount": 15.0},
        {"order_id": "O5", "status": "pending", "amount": 8.0},
    ]
)
# En Jupyter, el último valor se pinta: ya es la tabla, no un "plan".
pedidos_pd"""
        ),
        md(
            """`pedidos_pd` **es** la tabla. Si la dejas como última expresión, ves filas. No has llamado a nada parecido a `show()`.

La siguiente celda mira la forma y los tipos. `shape` y `dtypes` no lanzan ningún “job”: Pandas ya tiene los datos. Vas a ver `(5, 3)` y que `amount` es numérico. El índice será `[0, 1, 2, 3, 4]`."""
        ),
        code(
            """print("shape (filas, columnas):", pedidos_pd.shape)
print("tipos que Pandas adivinó:")
print(pedidos_pd.dtypes)
print("índice (Spark no tiene esto):", list(pedidos_pd.index))
# Estadísticos solo de las columnas numéricas
pedidos_pd.describe()"""
        ),
        md(
            """## Filtrar y agregar en Pandas

`pedidos_pd[condición]` recorre las filas **ahora** y te devuelve **otro** DataFrame, ya recortado. `len(...)` y `.sum()` son números inmediatos.

Al ejecutar: tres filas `paid`, suma de `amount` = `10 + 5 + 15` → **30**."""
        ),
        code(
            """# La máscara es una serie de True/False; el [] recorta en el acto.
paid_pd = pedidos_pd[pedidos_pd["status"] == "paid"]
print("tipo de paid_pd:", type(paid_pd))
print("len (filas paid):", len(paid_pd))  # 3, ya calculado
print("suma amount paid:", paid_pd["amount"].sum())  # 30.0
paid_pd"""
        ),
        md(
            """Una columna nueva se asigna con `df["col"] = ...` y **ya está** en el objeto. `groupby` aplasta filas (una por `status`) y el resultado también es inmediato: lo ves al ejecutar, sin `show()`."""
        ),
        code(
            """pedidos_pd = pedidos_pd.copy()  # por si reejecutas la celda
pedidos_pd["channel"] = "web"  # asignación eager: la columna existe ya
print(pedidos_pd[["order_id", "channel"]])

# count y suma de amount por estado; Pandas calcula al llegar aquí
pedidos_pd.groupby("status")["amount"].agg(["count", "sum"])"""
        ),
        md(
            """## El mismo dataset en PySpark

Mismas cinco filas, otra forma de pensar. `createDataFrame` no “guarda un Excel en Spark”. Guarda un **plan**: “si alguien pide estas filas, constrúyelas así”.

Al ejecutar vas a ver tres cosas distintas:

1. `print(pedidos_sp)` — un objeto (`DataFrame[order_id: string, …]`), **no** la tabla.
2. `printSchema()` — nombres y tipos. Aquí sí, porque los hemos creado en memoria y Spark los conoce.
3. `show()` — **ahora** sí pinta las cinco filas. `show` es una **acción**: obliga a ejecutar el plan."""
        ),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, lit, sum as fsum

# Row es una fila con nombre de campo. createDataFrame no "imprime" nada.
pedidos_sp = spark.createDataFrame(
    [
        Row(order_id="O1", status="paid", amount=10.0),
        Row(order_id="O2", status="cancelled", amount=20.0),
        Row(order_id="O3", status="paid", amount=5.0),
        Row(order_id="O4", status="paid", amount=15.0),
        Row(order_id="O5", status="pending", amount=8.0),
    ]
)
print("tipo:", type(pedidos_sp))
print("imprimir el objeto NO es la tabla (compara con pedidos_pd):")
print(pedidos_sp)
pedidos_sp.printSchema()  # contrato de columnas
pedidos_sp.show()  # acción: aquí aparecen las 5 filas"""
        ),
        md(
            """## Dónde se parecen y dónde no

Misma pregunta de negocio (“pedidos cobrados”), dos tiempos distintos.

| Qué haces | Pandas | PySpark |
|-----------|--------|---------|
| Ver las filas | el propio `df` / `head()` | `show()` (acción) |
| Número de filas | `len(df)` / `df.shape[0]` | `count()` (acción) |
| Tipos | `dtypes` | `printSchema()` |
| Índice 0,1,2… | sí | **no** |
| Filtrar | `df[df.col == x]` (ya calculado) | `filter` / `where` (solo alarga el plan) |
| Columna nueva | `df["c"] = ...` | `withColumn` (plan; hay que reasignar) |
| Agrupar | `groupby` (ya calculado) | `groupBy` + `agg` (plan hasta `show`) |
| Dónde viven | RAM de este proceso | particiones (aquí: cores del Codespace) |

La siguiente celda hace el `filter` de `paid`. El primer `print` **no** será una tabla de 3 filas. El `count()` sí dirá **3**, y entonces `show()` las pintará."""
        ),
        code(
            """# filter = transformación: Spark anota "más adelante, quédate con paid"
paid_sp = pedidos_sp.filter(col("status") == "paid")
print("después del filter, ¿es una tabla?")
print(paid_sp)  # objeto / plan, no 3 filas
print("count (ahora sí calcula):", paid_sp.count())  # acción → 3
paid_sp.show()"""
        ),
        md(
            """Misma agregación que en Pandas (suma de `amount` por `status`). Encadenamos `groupBy` + `agg` y solo al final `show()`. Si quitaras el `show()`, la celda no pintaría el cuadro: el plan se quedaría quieto."""
        ),
        code(
            """(
    pedidos_sp.groupBy("status")
    .agg(fsum("amount").alias("amount_sum"))
    .show()  # sin esto no ves el resultado
)"""
        ),
        md(
            """Columna nueva: en Spark **no** haces `df["channel"] = "web"` (eso pisa o falla; es el gesto de Pandas). Encadenas `withColumn` y **reasignas** (`pedidos_sp = ...`). `lit("web")` es “un literal igual en todas las filas”."""
        ),
        code(
            """# withColumn no muta: si no reasignas, pedidos_sp sigue sin channel
pedidos_sp = pedidos_sp.withColumn("channel", lit("web"))
pedidos_sp.select("order_id", "channel").show()"""
        ),
        md(
            """## El puente (y la trampa)

`toPandas()` copia **todas** las filas que le pidas al proceso Python. Con cinco pedidos no pasa nada. Con el fact de NovaShop (miles de líneas) o con un cluster (millones) te comes la RAM del Codespace.

La regla del curso: si quieres mirar en Pandas, primero `limit(...)`. Vas a ver un `DataFrame` de Pandas de **3** filas."""
        ),
        code(
            """# limit recorta el plan; toPandas materializa solo esas 3
muestra = pedidos_sp.limit(3).toPandas()
print(type(muestra))
muestra"""
        ),
        md(
            """## Transformación frente a acción

- **Transformación** (`filter`, `withColumn`, `groupBy`, `select`): alarga el plan. En Spark UI (puerto **4040**) no aparece un job nuevo.
- **Acción** (`show`, `count`, `collect`, `write`, `toPandas`): ejecuta. Aparece un job.

Spark no guarda el DataFrame como un Excel. Guarda un **plan**. Hasta una acción, la cocina está apagada.

**Siguiente:** abre el [lab](02-lab-sesion-spark.ipynb) y **crea tu** notebook."""
        ),
    ]


def m02() -> list:
    return [
        md(
            teoria_head(
                "M02 — Ingesta y preparación",
                """Hasta ahora las filas las inventábamos. A partir de aquí leemos **ficheros reales** de NovaShop (`data/raw/`).

La pregunta de este módulo no es “¿cuál es el API de `read.csv`?”. Es: **quién decide cómo se llama cada columna y de qué tipo es**. Eso lo puedes dejar en manos de Spark (mira el fichero y adivina) o lo puedes **escribir tú** (un contrato). En exploración vale lo primero. En un pipeline que mañana vuelve a correr, lo segundo.

Labs después: cargar → tipar → limpiar.""",
                "../M01-fundamentos-entorno/02-lab-sesion-spark.ipynb",
                "02-lab-ingesta-csv-json.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m02"),
        md(
            """## Qué hace Spark cuando lee un CSV “a pelo”

Un CSV es texto. No lleva tipos: no dice “esto es un timestamp” ni “esto es un decimal”. Si no le pasas un contrato, Spark **mira una muestra** del fichero y decide nombres y tipos. Eso es adivinar a partir de los datos (en la jerga: *inferir*).

En CSV, esa adivinación es especialmente vaga: casi todo acaba en **string**, aunque la columna se llame `OrderDate` o parezca un número. No es un fallo. Es “no me has dicho el tipo, no me la juego”.

Vamos a leer `orders.csv` solo con `header=True` (la primera línea son nombres). Al ejecutar verás:

- `count` → **800** (si sale 801, has contado la cabecera: falta el `header`).
- `printSchema()` → todas las columnas `string`, con nombres camelCase (`OrderId`, `OrderDate`…).
- `show(3)` → tres filas crudas, tal cual el fichero."""
        ),
        code(
            """# Sin .schema(...): Spark decide nombres (por la cabecera) y tipos (casi todo string).
orders_txt = spark.read.option("header", True).csv(str(RAW / "orders.csv"))
print("filas", orders_txt.count())  # 800
orders_txt.printSchema()  # espera string, string, string...
orders_txt.show(3, truncate=False)"""
        ),
        md(
            """## Dos JSON que no se leen igual

NovaShop trae dos JSON distintos. El método se llama igual (`.json`); el fichero no.

- `products.json` es **un solo documento**: un array `[ {...}, {...} ]`. Si Spark lee *línea a línea*, cada línea es un trozo de JSON roto y marca `_corrupt_record`. Por eso `multiLine=True`: “este fichero es un JSON entero, no un JSON por línea”.
- `events.jsonl` es **una línea = un objeto**. Ahí el valor por defecto va bien.

Al ejecutar: `products` **60**, `events` **2500**. En el schema de productos verás camelCase (`productId`, `listPrice`). Si `products` te sale ~362, falta el `multiLine`."""
        ),
        code(
            """# Array JSON (un documento) vs JSONL (un objeto por línea)
products = spark.read.option("multiLine", True).json(str(RAW / "products.json"))
events = spark.read.json(str(RAW / "events.jsonl"))
print("products", products.count(), "events", events.count())
products.printSchema()"""
        ),
        md(
            """## Tú escribes el contrato (y luego parseas las fechas)

Dejar que Spark adivine está bien para **mirar**. Para **producir**, escribes el contrato: lista de columnas, tipo de cada una, y si admite nulos. En el API eso es un `StructType` de `StructField`. No borra filas: solo dice “lee estas columnas así”.

En NovaShop el fichero mezcla dos textos de fecha: la mayoría `yyyy-MM-dd HH:mm:ss` y tres en `dd/MM/yyyy`. Si conviertes con **un** solo formato, esas tres se vuelven nulas. No desaparece el pedido: desaparece la fecha.

Qué vas a ver en la siguiente celda, en este orden:

1. Las tres fechas con `/` (suciedad deliberada).
2. Un contrato que **todavía deja `OrderDate` en string** (el texto crudo). Renombramos a `snake_case`.
3. `coalesce` de dos `to_timestamp`: “prueba ISO; si falla, prueba día/mes/año”.
4. `printSchema()` con `order_ts` ya en `timestamp`.
5. Nulos de fecha → **0**. El `count` sigue siendo **800**. Tipar no limpia claves ni tira filas: eso es el lab de calidad."""
        ),
        code(
            """from pyspark.sql.functions import col, coalesce, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# 1) Suciedad que el adivinador no te cuenta: tres fechas con barra
print("fechas raras (dd/mm/yyyy):")
orders_txt.where(col("OrderDate").contains("/")).select("OrderId", "OrderDate").show()

# 2) Contrato: nombres del FICHERO y tipos de lectura (aún texto).
#    True = la columna puede ser nula.
schema = StructType([
    StructField("OrderId", StringType(), True),
    StructField("CustomerId", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Channel", StringType(), True),
])

orders = (
    spark.read.option("header", True)
    .schema(schema)  # ya no adivina: usa esta lista
    .csv(str(RAW / "orders.csv"))
    .withColumnRenamed("OrderId", "order_id")
    .withColumnRenamed("CustomerId", "customer_id")
    .withColumnRenamed("OrderDate", "order_ts_raw")
    .withColumnRenamed("Status", "status")
    .withColumnRenamed("Channel", "channel")
    # 3) Dos formatos; coalesce se queda con el primero que no sea nulo
    .withColumn(
        "order_ts",
        coalesce(
            to_timestamp(col("order_ts_raw"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("order_ts_raw"), "dd/MM/yyyy"),
        ),
    )
    .drop("order_ts_raw")
)
orders.printSchema()
print("nulos de fecha", orders.where(col("order_ts").isNull()).count())  # 0
print("count sigue siendo", orders.count())  # 800: tipar ≠ filtrar"""
        ),
        md(
            """En los labs vas a repetir esta idea con líneas (enteros y decimales) y con eventos. El contrato lo escribes tú; Spark no tiene que “acertar” cada mañana.

**Siguiente:** [lab de ingesta](02-lab-ingesta-csv-json.ipynb) — creas tu notebook y cargas las cuatro fuentes."""
        ),
    ]


def m03() -> list:
    return [
        md(
            teoria_head(
                "M03 — Transformación",
                """Ya no estamos “leyendo el fichero”. Estamos **derivando columnas de negocio** a partir de las que ya tienes. La regla no es un `for` fila a fila: es una expresión que Spark aplica a toda la columna (`withColumn`).

Demo con cuatro líneas en memoria. No hace falta el staging: queremos ver el fallo de un descuento sucio *antes* de taparlo.""",
                "../M02-ingesta-preparacion/04-lab-calidad-limpieza.ipynb",
                "02-lab-enriquecimiento.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m03"),
        md(
            """## Una fórmula es una columna

GMV de línea = `qty * unit_price * (1 - discount)`. Si `discount` es `0.10`, quitas el 10 %. Si en el raw alguien escribió `1.50` (ciento cincuenta por ciento), `(1 - 1.50)` es negativo y el GMV **sale negativo**. No es un bug de Spark: es suciedad que la fórmula reproduce.

Al ejecutar verás cuatro filas. `O2` tiene `discount=1.50` y `gmv_line` negativo. Eso es lo que el lab de reglas tapa; ahora queremos **verlo**."""
        ),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, when, lower, least, lit

lineas = spark.createDataFrame([
    Row(order_id="O1", qty=2, unit_price=10.0, discount=0.10, status="paid", channel="WEB"),
    Row(order_id="O2", qty=1, unit_price=80.0, discount=1.50, status="cancelled", channel="marketplace"),
    Row(order_id="O3", qty=3, unit_price=5.0, discount=0.0, status="paid", channel="app"),
    Row(order_id="O4", qty=1, unit_price=20.0, discount=0.0, status="pending", channel="store"),
])
# withColumn añade (o pisa) una columna. No hace falta un for.
crudo = lineas.withColumn(
    "gmv_line",
    col("qty") * col("unit_price") * (1 - col("discount")),
)
crudo.select("order_id", "discount", "gmv_line").show()  # O2 negativo"""
        ),
        md(
            """## Tres reglas encadenadas (y recalcular al final)

En el lab harás esto sobre el fact real. Aquí, sobre las 4 filas:

1. **Capar** el descuento a 1 (`least(discount, 1)`): no puedes descontar más del 100 %.
2. **Normalizar** el canal: `WEB`/`App`/`marketplace` no sirven para un `groupBy` limpio. Nos quedamos con `web`, `app`, `store` u `other`.
3. **Marcar** lo cobrable (`status == paid`). No filtres aún: el fact guarda todas las líneas; el flag decide en los KPIs.

Importante: si capas `discount` *después* de haber calculado `gmv_line` y no vuelves a calcular, el negativo **sigue**. Por eso el GMV se escribe **al final** de la cadena.

Al ejecutar: `O2` ya no tiene GMV negativo; `channel_norm` es `web` / `other` / `app` / `store`; `is_billable` es true solo en `paid`."""
        ),
        code(
            """fact = (
    crudo
    # 1) tope de descuento (no muta gmv_line todavía)
    .withColumn("discount", least(col("discount"), lit(1.0)))
    # 2) canal en minúsculas y set cerrado
    .withColumn(
        "channel_norm",
        when(lower(col("channel")).isin("web", "app", "store"), lower(col("channel")))
        .otherwise(lit("other")),
    )
    # 3) flag; el fact sigue teniendo las 4 filas
    .withColumn("is_billable", col("status") == "paid")
    # 4) ahora sí: GMV con el discount ya capado
    .withColumn("gmv_line", col("qty") * col("unit_price") * (1 - col("discount")))
)
fact.select(
    "order_id", "channel", "channel_norm", "discount", "gmv_line", "is_billable"
).show()"""
        ),
        md(
            """No uses `collect()` / `toPandas()` del fact entero. Si quieres mirar, `limit(20).toPandas()`.

**Siguiente:** [lab de enriquecimiento](02-lab-enriquecimiento.ipynb)."""
        ),
    ]


def m04() -> list:
    return [
        md(
            teoria_head(
                "M04 — Joins y KPIs",
                """Un número de negocio mentiroso casi siempre viene de **mezclar dos tamaños de fila**, no de un `sum` mal escrito.

Antes de cruzar nada, fíjate en esto (como un ticket de supermercado):

- Un **pedido** (`order_id`) es el ticket entero: “O1”.
- Una **línea** es un producto dentro del ticket: “100 € de auriculares” y “50 € de cable” pueden ser **el mismo** pedido.
- Un **cliente** vive en otra lista. Si la línea apunta a un id que no está en esa lista, es un **huérfano** (en NovaShop, los `CX*`).

En las celdas de abajo el juguete es pequeño a propósito: **un pedido con dos líneas**, otro pedido de una línea, y un huérfano. Así se ve la diferencia. Luego el lab usa NovaShop entero.""",
                "../M03-transformacion-datos/03-lab-reglas-negocio.ipynb",
                "02-lab-joins.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m04"),
        md(
            """## Las dos tablas, en bruto

`clientes` tiene **una fila por persona**. `lineas` tiene **una fila por producto vendido**, no por pedido.

Al ejecutar verás 2 clientes y **4 líneas**. `O1` aparece **dos veces** (100 € y 50 €): eso no son dos ventas de compañía, es **un** ticket con dos productos. `O3` / `CX9` no tiene ficha en `clientes`."""
        ),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, sum as fsum, countDistinct, avg

clientes = spark.createDataFrame([
    Row(customer_id="C1", country="ES"),
    Row(customer_id="C2", country="FR"),  # no compra en este juguete; da igual
])
# gmv_line = dinero de ESA línea (un producto), no del pedido entero
lineas = spark.createDataFrame([
    Row(order_id="O1", customer_id="C1", gmv_line=100.0, is_billable=True),
    Row(order_id="O1", customer_id="C1", gmv_line=50.0, is_billable=True),   # mismo pedido
    Row(order_id="O2", customer_id="C1", gmv_line=30.0, is_billable=True),
    Row(order_id="O3", customer_id="CX9", gmv_line=999.0, is_billable=True),  # huérfano
])
print("clientes (1 fila = 1 persona)")
clientes.show()
print("lineas (1 fila = 1 producto; O1 está dos veces)")
lineas.show()"""
        ),
        md(
            """## Cruzar: “¿esta línea tiene cliente de verdad?”

Un join no suma dinero. Solo pregunta, **línea a línea**: ¿el `customer_id` está en `clientes`?

- **inner** — me quedo solo si hay emparejamiento. `CX9` (y sus 999 €) **desaparecen**. Cuenta: **3** (las dos de `O1` y la de `O2`).
- **left** — me quedo con **todas** las líneas. `CX9` sigue, `country` sale `null`. Cuenta: **4**. Sirve para *ver* cuánto se cae, no para decir “vendimos 999 a un cliente”.
- **left_anti** — “líneas cuyo cliente **no** está en la lista”. Es la foto del huérfano. Verás `O3`.

Al ejecutar: inner **3**, left **4**, anti = `O3`."""
        ),
        code(
            """print("inner (fuera el huérfano)", lineas.join(clientes, "customer_id", "inner").count())
print("left  (siguen las 4)     ", lineas.join(clientes, "customer_id", "left").count())
print("quién no está en clientes:")
lineas.join(clientes, "customer_id", "left_anti").show()"""
        ),
        md(
            """## `sales` no es “las ventas del pedido”

En los labs llamamos `sales` a: líneas **cobrables** (`is_billable`) **con cliente conocido** (inner).

Eso **no** agrupa. Sigue habiendo **una fila por producto**. `O1` sigue saliendo dos veces. El nombre engaña: no es un ticket cerrado, es el recorte “estas líneas sí cuentan para dinero atribuible”.

Al ejecutar: **3** filas, GMV de línea 100 / 50 / 30. El 999 ya no está."""
        ),
        code(
            """# Recorte, no agregación: mismas 3 líneas, ahora con country
sales = lineas.join(clientes, "customer_id", "inner").where(col("is_billable"))
print("filas en sales (sigue siendo grano LÍNEA):", sales.count())
sales.show()"""
        ),
        md(
            """## El ticket medio: no hagas la media de las filas

Pregunta de negocio: “¿cuánto deja de media **un pedido**?”

En `sales` hay **3 productos** y solo **2 tickets** (`O1` = 150 €, `O2` = 30 €).

| Cálculo | Qué está promediando | Número |
|---------|----------------------|-------:|
| `avg(gmv_line)` | las **3 líneas** (100, 50, 30) | **60** ← mentira útil |
| `sum(gmv_line) / countDistinct(order_id)` | los **2 pedidos** (150 y 30) | **90** ← ticket medio |

`avg` no sabe qué filas son el mismo `order_id`. Por eso la regla del curso es siempre:

`GMV = sum(gmv_line)` y `pedidos = countDistinct(order_id)` y `AOV = GMV / pedidos`.

El `groupBy("country")` no cambia de grano: es **el mismo 180 €** partido por país (aquí todo es ES).

Al ejecutar: una fila `gmv=180`, `orders=2`, `aov=90`; `avg_linea=60`; país ES = 180."""
        ),
        code(
            """# Lo que NO hay que usar para el ticket:
sales.agg(avg("gmv_line").alias("avg_linea")).show()  # 60: media de productos

# Lo que SÍ: dinero total y cuántos tickets distintos
kpis = sales.agg(
    fsum("gmv_line").alias("gmv"),                 # 180
    countDistinct("order_id").alias("orders"),     # 2  (no 3)
)
kpis = kpis.withColumn("aov", col("gmv") / col("orders"))  # 90
kpis.show()

# Mismo 180, cortado por país (sigue siendo suma de líneas)
sales.groupBy("country").agg(fsum("gmv_line").alias("gmv")).show()"""
        ),
        md(
            """En NovaShop pasa lo mismo a lo grande: `sales` son miles de **líneas**; el AOV divide por pedidos distintos, no por `count()` de filas.

**Siguiente:** [lab de joins](02-lab-joins.ipynb) sobre el dataset real."""
        ),
    ]


def m05() -> list:
    return [
        md(
            teoria_head(
                "M05 — Ranking y acumulados (ventanas)",
                """En M04 `groupBy` te dio **un número por grupo** (GMV por país, GMV por cliente). Eso está bien para un cuadro de mando. Está mal si la pregunta es: “¿cuál fue el **2.º** pedido de este cliente?” o “¿cuánto llevaba gastado **cuando** compró esto?”.

Para eso necesitas **seguir viendo cada fila** y, a su lado, un dato que mira a los vecinos (el mismo cliente, ordenados por fecha). Eso es una **ventana**.

El juguete: dos clientes, dos compras cada uno. Mini a propósito. Luego el lab usa NovaShop.""",
                "../M04-integracion-agregacion/04-lab-segmentacion.ipynb",
                "02-lab-ranking-ventana.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m05"),
        md(
            """## Cuatro compras, todavía sueltas

Cada fila es **un pedido** (un ticket). `C1` compró en enero (10 €) y en febrero (30 €). `C2` igual, otras fechas.

Al ejecutar: **4 filas**. No hay ranking ni acumulado todavía. Si `count` no es 4, para."""
        ),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col, row_number, sum as fsum
from pyspark.sql.window import Window

# 1 fila = 1 pedido (no una línea de producto)
hist = spark.createDataFrame([
    Row(customer_id="C1", order_id="O1", order_n_ts="2024-01-01", gmv=10.0),
    Row(customer_id="C1", order_id="O2", order_n_ts="2024-02-01", gmv=30.0),
    Row(customer_id="C2", order_id="O3", order_n_ts="2024-01-15", gmv=5.0),
    Row(customer_id="C2", order_id="O4", order_n_ts="2024-03-01", gmv=8.0),
])
print("filas", hist.count())
hist.orderBy("customer_id", "order_n_ts").show()"""
        ),
        md(
            """## `groupBy`: te quedas con el resumen y **tiras** el detalle

Si preguntas “¿cuánto ha gastado cada cliente **en total**?”, `groupBy("customer_id")` es la herramienta. Spark junta las filas del mismo cliente y escribe **una** fila de salida.

Eso es lo que aquí llamamos “aplastar”: no es que Spark rompa nada. Es que **dejas de tener O1 y O2**. Solo queda `C1 → 40 €`. Ya no puedes decir qué pedido fue el primero.

Al ejecutar: **2 filas** (una por cliente). Las columnas `order_id` y `order_n_ts` **no están**. No es un bug: las has fundido en la suma."""
        ),
        code(
            """# Una fila por cliente. O1 y O2 ya no existen como filas.
totales = hist.groupBy("customer_id").agg(fsum("gmv").alias("gmv_total"))
print("filas después del groupBy:", totales.count())  # 2
totales.show()"""
        ),
        md(
            """## Ventana: mismas 4 filas, con una columna extra que **mira al lado**

La pregunta ahora es otra: “en **esta** compra, ¿cuánto llevaba gastado este cliente **hasta aquí**?” y “¿es su pedido nº 1 o nº 2?”.

Una **ventana** no junta filas. Recorre cada fila, mira el vecindario que tú defines, escribe un número **en esa misma fila**.

El vecindario se declara así:

- `partitionBy("customer_id")` — “los vecinos son **solo este cliente**”. `C1` no ve los pedidos de `C2`. Cuando acaba `C1`, el contador **vuelve a 1** en `C2`.
- `orderBy("order_n_ts")` — “dentro de ese cliente, ordena por fecha”. Sin orden, “acumulado” no significa nada (¿hasta cuándo?).

Al ejecutar: **siguen 4 filas**. `C1` tiene `order_n` 1 y 2; `gmv_running` pasa de 10 a **40**. `C2` **empieza otra vez** en 1 (5, luego 13). Si `C2` saliera 3 y 4, olvidaste el `partitionBy`."""
        ),
        code(
            """# Vecindario: mismo cliente, ordenado por fecha
w = Window.partitionBy("customer_id").orderBy("order_n_ts")
ranked = (
    hist.withColumn("order_n", row_number().over(w))   # 1.er, 2.º pedido de ESE cliente
    .withColumn("gmv_running", fsum("gmv").over(w))  # suma desde el 1.er pedido hasta ESTE
)
print("filas (tienen que seguir siendo 4):", ranked.count())
ranked.orderBy("customer_id", "order_n").show()"""
        ),
        md(
            """## `partitionBy` de la ventana no es una carpeta en disco

Se parecen las palabras y no son lo mismo:

| Qué escribes | Qué hace |
|--------------|----------|
| `Window.partitionBy("customer_id")` | Recorta el **vecindario** del cálculo (por cliente). No crea carpetas. |
| `repartition` / `write.partitionBy` (M06–M07) | Baraja tareas o **escribe** `order_month=2024-01/` en disco. |

Si `gmv_running` **baja** dentro del mismo cliente, el `orderBy` de la ventana no es la fecha (o está al revés)."""
        ),
        md(
            """## Prueba tú (en este mismo notebook)

1. Copia la celda de la ventana y **borra** `.partitionBy("customer_id")`. Deja solo `Window.orderBy("order_n_ts")`. `order_n` será 1,2,3,4 **en toda la empresa**. `C2` ya no vuelve a 1.
2. En la ventana **con** `partitionBy`, cambia a `.orderBy(col("order_n_ts").desc())`. El primer `gmv_running` de `C1` dejará de ser 10: estás acumulando desde el pedido **más nuevo**.

Si no cambias nada y solo re-ejecutas, no has comprobado la ventana: has vuelto a ver el mismo `show`."""
        ),
        md("**Siguiente:** [lab de ranking](02-lab-ranking-ventana.ipynb). Ahí el top 10 y el top 3 por cliente son la misma idea, a tamaño NovaShop."),
    ]


def m06() -> list:
    return [
        md(
            teoria_head(
                "M06 — Lazy, plan y cache",
                """En M01 viste que `filter` no pinta filas. Aquí lo miramos por dentro: el **plan** (lo que Spark *haría*) frente al **job** (lo que *hace* cuando lanzas una acción). Y el `cache`: no es magia; es “guarda el resultado de una acción para no repetir el plan”.""",
                "../M05-analisis-avanzado/03-lab-acumulados.ipynb",
                "02-lab-explain-dag.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m06"),
        md(
            """## Encadenar no ejecuta

Montamos 20 filas y encadenamos dos `where` y un `select`. Eso solo alarga el plan.

Al ejecutar:

- el primer `print` es el objeto (sin número);
- `count()` sí dispara un job y te da un entero;
- `explain("formatted")` imprime el mapa: busca un *Scan* / *Filter*. No hace falta traducir cada operador.

Si tienes Spark UI en el **4040**, el Job Id no debería subir con el primer `print`; sí con el `count`."""
        ),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col

base = spark.createDataFrame(
    [Row(x=i, canal="web" if i % 2 == 0 else "app") for i in range(20)]
)
# Tres transformaciones: todavía no hay job
planned = base.where(col("x") > 3).where(col("canal") == "web").select("x")
print("sin acción (solo el objeto):", planned)
print("con count (ahora sí):", planned.count())
planned.explain("formatted")  # mapa, no el resultado"""
        ),
        md(
            """## El cache no se llena al escribir `.cache()`

`cache()` marca el DataFrame: “la **próxima** acción, guarda el resultado en memoria”. Si no hay `count`/`show`, la pestaña Storage de Spark UI sigue vacía.

Al ejecutar: el primer `count` materializa; el segundo debería leer de ahí. Luego `unpersist()` para no dejar basura en el Codespace. En local, con 20 filas, el cronómetro a veces no se inmuta: lo que importa es Storage, no el stopwatch."""
        ),
        code(
            """warm = planned.cache()  # aún no hay nada en Storage
print("1º count (llena el cache)", warm.count())
print("2º count (debería leer cache)", warm.count())
warm.unpersist()  # suelta la memoria"""
        ),
        md(
            """**Siguiente:** [lab de explain](02-lab-explain-dag.ipynb) sobre el fact real y la UI."""
        ),
    ]


def m07() -> list:
    return [
        md(
            teoria_head(
                "M07 — Parquet y partición de negocio",
                """El pipeline no acaba en un `show()`. Acaba en un **directorio** que otro proceso (o tú mañana) puede leer sin repetir joins.

Dos “particiones” que se confunden:

- `repartition` (M06) baraja **memoria** entre tareas.
- `partitionBy` en el `write` crea **carpetas en disco** (`order_month=2024-01`, …). Al filtrar un mes, Spark puede **no abrir** las demás.""",
                "../M06-optimizacion-ejecucion/03-lab-cache-particionado.ipynb",
                "02-lab-parquet-layout.ipynb",
            )
        ),
        *boot_cells("novashop-clase-m07"),
        md(
            """## Escribes carpetas, no un Excel

Tres filas de juguete, dos meses. `write.partitionBy("order_month").parquet(...)` deja un directorio por mes. `overwrite` hace el resultado **idempotente**: si vuelves a ejecutar, no duplicas.

Al leer solo enero, `explain` debería mencionar `2024-01` (o un *PartitionFilters*). El count de enero es **2**; el total, **3**."""
        ),
        code(
            """from pyspark.sql import Row
from pyspark.sql.functions import col

demo = spark.createDataFrame([
    Row(order_id="O1", order_month="2024-01", gmv=10.0),
    Row(order_id="O2", order_month="2024-01", gmv=20.0),
    Row(order_id="O3", order_month="2024-02", gmv=5.0),
])
dest = CURATED / "_demo_sales"
CURATED.mkdir(parents=True, exist_ok=True)
# Carpetas en disco, no un único fichero "datos.parquet"
demo.write.mode("overwrite").partitionBy("order_month").parquet(str(dest))
print("carpetas:", sorted(p.name for p in dest.iterdir() if p.is_dir()))

enero = spark.read.parquet(str(dest)).where(col("order_month") == "2024-01")
enero.explain("formatted")  # busca 2024-01 / PartitionFilters
print("enero", enero.count(), "total", spark.read.parquet(str(dest)).count())"""
        ),
        md("**Siguiente:** [lab de parquet](02-lab-parquet-layout.ipynb) sobre el fact real."),
    ]
