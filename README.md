# Python para tratamiento de datos con PySpark

Trabajas sobre **NovaShop**: una tienda online de laboratorio (pedidos, catálogo, eventos de navegación). No es un producto real ni un clúster. Es el **mismo** escenario en todos los módulos, para que el pipeline se vaya construyendo: `data/raw/` → staging → fact → Parquet analítico. No hay proyecto final.

Todo el curso vive en **notebooks**. Spark corre en este Codespace en modo **`local[*]`** (los cores de esta máquina). No hay YARN, Databricks ni streaming.

Este curso es para ti si ya usas Python (y probablemente Pandas) y necesitas **escalar** el procesamiento. No se piden Big Data ni Spark de antemano. Si Python está flojo: [Python de bolsillo](notebooks/M00-entorno-notebooks/03-python-recordatorio.ipynb).

Al terminar serás capaz de:

- Trabajar con PySpark de forma autónoma (`SparkSession`, DataFrames, acciones frente a transformaciones).
- Construir pipelines completos: ingesta → transformación → salida.
- Procesar volúmenes grandes de forma eficiente (aquí en `local[*]`) y razonar el salto a clúster.
- Aplicar transformaciones, joins y agregaciones de negocio reales.
- Entender el modelo de ejecución de Spark (lazy, DAG) y aplicar optimizaciones básicas (cache, particionado).
- Dejar datos listos para analítica avanzada o machine learning.

Cada concepto se introduce y se aplica en laboratorio sobre el **mismo** dominio. No hay proyecto final: el pipeline *es* la suma de los labs (carga → transformación → validación).

## Cómo va cada módulo

1. Abres la **teoría** (`01-teoria.ipynb`): lees y **ejecutas** las celdas aquí mismo (en clase, juntos).
2. Abres el **lab** (`0N-lab-….ipynb`): es el guion. **Crea tu propio notebook** en `notebooks/trabajo/` y ve creando celdas Markdown (qué y por qué) + código. Ejecutas, compruebas, mejoras.

Empieza por el Lab 0: entorno, fork, Codespace y qué es un notebook.

Índice de ficheros: [notebooks/README.md](notebooks/README.md). Dataset: [data/README.md](data/README.md). Codespace: [infra/README.md](infra/README.md).

## Temario

### M00 — Entorno y notebooks

Antes de Spark: el cuaderno, el fork y el Codespace. Así trabajas el resto del curso.

- Qué es un notebook (celdas Markdown y de código) y cómo ejecutarlo.
- Fork del repo y arranque del Codespace (Java 17, PySpark, kernel **Python (NovaShop)**).
- Laboratorio: creas `notebooks/trabajo/M00-01-mi-primer-notebook.ipynb` y compruebas rutas y dataset.
- Recordatorio de Python (si no tienes base): tipos simples y compuestos (`list`, `tuple`, `dict`, `set`), `if`, `None`, `import`.

→ [teoría](notebooks/M00-entorno-notebooks/01-teoria.ipynb) · [lab](notebooks/M00-entorno-notebooks/02-lab-primer-notebook.ipynb) · [Python de bolsillo](notebooks/M00-entorno-notebooks/03-python-recordatorio.ipynb)

### M01 — Fundamentos y entorno

Spark como motor de cómputo, no como base de datos. El puente si vienes de Pandas.

**Contenidos**

- Qué es Spark y cuándo usarlo.
- Diferencia con el procesamiento tradicional (Pandas): RAM, índice, cuándo se calcula.
- `SparkSession` y modelo de ejecución (transformación vs acción).

**Laboratorio**

- Arranque del entorno.
- Creación (o reuso) de la sesión Spark.
- Primer DataFrame y ejecución básica (`printSchema`, `show`, `count`).

→ [teoría](notebooks/M01-fundamentos-entorno/01-teoria.ipynb) · [lab](notebooks/M01-fundamentos-entorno/02-lab-sesion-spark.ipynb)

### M02 — Ingesta y preparación de datos

Lees las fuentes reales de NovaShop. Quién decide nombres y tipos: Spark puede adivinarlos; para producir, escribes tú el contrato.

**Contenidos**

- Lectura de datos (CSV, JSON y JSONL).
- Inferencia de schema frente a schema explícito.
- Tipos de datos (string, timestamp, decimal).
- Calidad y limpieza.

**Laboratorios**

- Ingesta del dataset real (clientes, pedidos, catálogo, eventos).
- Exploración de estructura.
- Normalización de columnas y tipos.
- Filtrado de datos inválidos y escritura del staging.

→ [teoría](notebooks/M02-ingesta-preparacion/01-teoria.ipynb) · [ingesta](notebooks/M02-ingesta-preparacion/02-lab-ingesta-csv-json.ipynb) · [schema](notebooks/M02-ingesta-preparacion/03-lab-schema-tipos.ipynb) · [calidad](notebooks/M02-ingesta-preparacion/04-lab-calidad-limpieza.ipynb)

**Extra — JSON anidado y schema que cambia**

Los alumnos que vienen de CMS/CRM: un JSON de varios niveles, bajarlo a columnas, enriquecerlo y volver a un documento (el que comería una app o un `mongoimport`). Y dos dumps del mismo cliente (2023 plano vs 2024 anidado) cuando la migración se quedó a medias.

No hay Mongo en el Codespace. El artefacto es JSONL. No sustituye el pipeline de pedidos.

→ [teoría](notebooks/M08-json-anidado-schema/01-teoria.ipynb) · [lab](notebooks/M08-json-anidado-schema/02-lab-json-anidado-schema.ipynb)

### M03 — Transformación de datos

La regla de negocio es una **columna**, no un `for`. Enriqueces el dataset y encadenas transformaciones.

**Contenidos**

- Operaciones sobre DataFrames (`select`, `withColumn`).
- Filtros y expresiones.
- Lógica de negocio aplicada a datos (GMV, canal, cobrable).

**Laboratorios**

- Enriquecimiento con nuevas columnas (`gmv_line`, `order_month`).
- Aplicación de reglas de negocio (capar descuento, normalizar canal).
- Transformaciones encadenadas y persistencia de `fact_lines`.

→ [teoría](notebooks/M03-transformacion-datos/01-teoria.ipynb) · [enriquecimiento](notebooks/M03-transformacion-datos/02-lab-enriquecimiento.ipynb) · [reglas](notebooks/M03-transformacion-datos/03-lab-reglas-negocio.ipynb)

### M04 — Integración y agregación

Cruzas fuentes y calculas métricas. Un KPI mentiroso casi siempre es un join mal elegido.

**Contenidos**

- Joins entre datasets (inner, left, anti).
- Agrupaciones (`groupBy`).
- Cálculo de métricas.

**Laboratorios**

- Integración de múltiples fuentes (clientes + ventas).
- Cálculo de KPIs (GMV, pedidos, ticket medio, cancelación).
- Agregaciones y métricas de negocio.
- Segmentación de datos (`low` / `mid` / `high`).

→ [teoría](notebooks/M04-integracion-agregacion/01-teoria.ipynb) · [joins](notebooks/M04-integracion-agregacion/02-lab-joins.ipynb) · [KPIs](notebooks/M04-integracion-agregacion/03-lab-kpis.ipynb) · [segmentación](notebooks/M04-integracion-agregacion/04-lab-segmentacion.ipynb)

### M05 — Análisis avanzado

`groupBy` aplasta filas. Una ventana calcula y **conserva** el detalle (pedido a pedido, cliente a cliente).

**Contenidos**

- Window functions.
- Particionado lógico de la ventana (`partitionBy` ≠ ficheros en disco).
- Ranking y acumulados.

**Laboratorio**

- Análisis por entidad (cliente / usuario).
- Ranking de resultados (top clientes, top productos).
- Cálculo de métricas acumuladas en el tiempo.

→ [teoría](notebooks/M05-analisis-avanzado/01-teoria.ipynb) · [ranking](notebooks/M05-analisis-avanzado/02-lab-ranking-ventana.ipynb) · [acumulados](notebooks/M05-analisis-avanzado/03-lab-acumulados.ipynb)

### M06 — Optimización y ejecución

Sin acción no hay job. Lees el plan y decides cuándo cachear o reparticionar.

**Contenidos**

- Evaluación lazy.
- Plan de ejecución (DAG).
- Uso de cache y particionado.

**Laboratorio**

- Inspección del plan de ejecución (`explain`, Spark UI).
- Comparativa de rendimiento (con y sin cache).
- Aplicación de optimizaciones básicas (`repartition`).

→ [teoría](notebooks/M06-optimizacion-ejecucion/01-teoria.ipynb) · [explain](notebooks/M06-optimizacion-ejecucion/02-lab-explain-dag.ipynb) · [cache](notebooks/M06-optimizacion-ejecucion/03-lab-cache-particionado.ipynb)

### M07 — Persistencia de datos

El pipeline acaba en un directorio que otro proceso puede leer mañana.

**Contenidos**

- Escritura en formatos eficientes (Parquet).
- Organización de datos (`partitionBy` en disco).
- Buenas prácticas (overwrite, prune de partición, no CSV como almacén).

**Laboratorio**

- Exportación del dataset transformado.
- Estructuración para consumo analítico (`data/curated/sales_analytics`).

→ [teoría](notebooks/M07-persistencia-datos/01-teoria.ipynb) · [lab](notebooks/M07-persistencia-datos/02-lab-parquet-layout.ipynb)

## Resultado

Construyes un flujo completo de procesamiento en PySpark: de las fuentes sucias de NovaShop a un Parquet analítico. Entiendes el uso práctico **y** el comportamiento interno (plan, acciones, joins), para llevarlo a un entorno profesional.
