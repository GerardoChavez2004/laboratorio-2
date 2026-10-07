# Databricks notebook source
# MAGIC %md
# MAGIC # Laboratorio 2. El tablero no cuadra
# MAGIC
# MAGIC Alguien dejó un pipeline que dice éxito. Finanzas dice que las ventas no cuadran con el archivo.
# MAGIC
# MAGIC Su trabajo no es seguir una receta. Es dejar `oro_fact_ventas` usable y explicar qué apartaron.
# MAGIC
# MAGIC Reglas del negocio, nada más:
# MAGIC
# MAGIC - Una fila por `venta_id`.
# MAGIC - Solo entran las tiendas 1, 2 y 3.
# MAGIC - `monto` tiene que ser número. Si viene con coma, se interpreta. Si no se puede, se aparta.
# MAGIC - Una columna que no estaba en el contrato no entra al hecho.
# MAGIC - Lo apartado queda en `cuarentena_ventas`, con un motivo.
# MAGIC - Correr el notebook otra vez no duplica el hecho.
# MAGIC
# MAGIC No editen el CSV. Si una prueba falla, el hallazgo es de ustedes.

# COMMAND ----------

# 1. Limpiar widgets anteriores para eliminar el valor guardado
dbutils.widgets.removeAll()

# 2. Definir la ruta correcta hacia el archivo cargado en la carpeta
ruta = "file:/Workspace/Users/202401062@ujmd.edu.sv/Drafts/Lab 2/ventas_export.csv"

# 3. Cargar el archivo y crear la vista temporal 'bronce_ventas'
bronce = spark.read.option("header", True).option("inferSchema", False).csv(ruta)
bronce.createOrReplaceTempView("bronce_ventas")

print("Filas en el archivo:", bronce.count())
print("Columnas:", bronce.columns)

# COMMAND ----------

# 1. Limpiar widgets guardados
dbutils.widgets.removeAll()

# 2. Definir la ruta real a tu CSV
ruta = "file:/Workspace/Users/202401062@ujmd.edu.sv/Drafts/Lab 2/ventas_export.csv"

# 3. Cargar el archivo y registrar la vista 'bronce_ventas'
bronce = spark.read.option("header", True).option("inferSchema", False).csv(ruta)
bronce.createOrReplaceTempView("bronce_ventas")

# 4. Crear la dimensión de tiendas (incluida en el notebook original)
spark.sql("""
create or replace temp view dim_tienda as
select * from values
  (1, 'Centro'),
  (2, 'Escalon'),
  (3, 'Soyapango')
as t(tienda_id, nombre)
""")

print("Filas en el archivo:", bronce.count())
print("Columnas:", bronce.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## El pipeline que heredaron
# MAGIC
# MAGIC Esta celda es el flujo roto. Léanla, no la copien como entrega. Carga todo, no aparta nada y si la corren dos veces duplica.

# COMMAND ----------

spark.sql("""
create or replace temp view oro_roto as
select * from bronce_ventas
""")
display(spark.sql("select count(*) as filas_oro_roto from oro_roto"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Su pipeline
# MAGIC
# MAGIC Reemplacen esta celda. Tiene que crear `oro_fact_ventas` y `cuarentena_ventas`.
# MAGIC
# MAGIC `oro_fact_ventas`: `venta_id`, `fecha`, `tienda_id`, `producto_id`, `cantidad`, `monto`, `medio_pago`.
# MAGIC
# MAGIC `cuarentena_ventas`: las que no entran, con una columna `motivo`.

# COMMAND ----------

# Escriban aquí el flujo. No usen el CSV editado.
# spark.sql("create or replace temp view oro_fact_ventas as ...")
# spark.sql("create or replace temp view cuarentena_ventas as ...")
# 1. Evaluación, limpieza y etiquetado de reglas de negocio
# 1. Evaluación, limpieza de comas/puntos y etiquetado de reglas
spark.sql("""
create or replace temp view ventas_evaluadas as
with datos_parseados as (
    select
        *,
        try_cast(regexp_replace(cast(monto as string), ',', '.') as double) as monto_clean,
        try_cast(tienda_id as int) as tienda_id_clean
    from bronce_ventas
),
datos_deduplicados as (
    select
        *,
        row_number() over (partition by venta_id order by fecha desc) as _rn
    from datos_parseados
)
select
    *,
    case
        when tienda_id_clean not in (1, 2, 3) or tienda_id_clean is null then 'Tienda no permitida'
        when monto_clean is null then 'Monto no numérico o inválido'
        when _rn > 1 then 'Venta_id duplicado'
        else null
    end as motivo
from datos_deduplicados
""")

# 2. Tabla de hechos en oro (cumple estrictamente el contrato de 7 columnas)
spark.sql("""
create or replace temp view oro_fact_ventas as
select
    venta_id,
    fecha,
    tienda_id_clean as tienda_id,
    producto_id,
    cantidad,
    monto_clean as monto,
    medio_pago
from ventas_evaluadas
where motivo is null
""")

# 3. Tabla de cuarentena con el motivo de rechazo
spark.sql("""
create or replace temp view cuarentena_ventas as
select
    *
from ventas_evaluadas
where motivo is not null
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pruebas
# MAGIC
# MAGIC No dicen cómo arreglarlo. Dicen si ya quedó. Las seis tienen que decir PASÓ.

# COMMAND ----------

oro = spark.table("oro_fact_ventas")
cuarentena = spark.table("cuarentena_ventas")
bronce = spark.table("bronce_ventas")

def prueba(nombre, ok):
    print(("PASÓ  " if ok else "FALLÓ ") + nombre)
    return ok

duplicados = spark.sql("""
select count(*) c from (
  select venta_id from oro_fact_ventas
  group by venta_id having count(*) > 1
)""").first()[0]

tiendas_malas = spark.sql("""
select count(*) c from oro_fact_ventas
where try_cast(tienda_id as int) not in (1, 2, 3)
""").first()[0]

montos_malos = spark.sql("""
select count(*) c from oro_fact_ventas
where try_cast(monto as double) is null
""").first()[0]

columnas_de_mas = [c for c in oro.columns if c.lower() == "canal"]
sin_motivo = "motivo" not in [c.lower() for c in cuarentena.columns]
oro_inflado = oro.count() >= bronce.count()
cuarentena_vacia = cuarentena.count() == 0

ok = []
ok.append(prueba("una fila por venta_id", duplicados == 0))
ok.append(prueba("solo tiendas 1, 2 y 3", tiendas_malas == 0))
ok.append(prueba("monto numérico en el hecho", montos_malos == 0))
ok.append(prueba("canal no entra al hecho", len(columnas_de_mas) == 0))
ok.append(prueba("cuarentena con motivo y con filas", (not sin_motivo) and (not cuarentena_vacia)))
ok.append(prueba("oro menor que el archivo", not oro_inflado))
print("-")
print("LISTO" if all(ok) else "SIGAN. Hay pruebas en rojo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafío o reto
# MAGIC
# MAGIC Cuando las seis digan PASÓ, respondan en esta celda, sin mirar la guía:
# MAGIC
# MAGIC 1. ¿Qué pasó con la misma venta llegada dos veces?
# MAGIC
# MAGIC Se deduplicó mediante la función de ventana ROW_NUMBER() OVER (PARTITION BY venta_id ORDER BY fecha DESC). La primera instancia procesada se guardó en oro_fact_ventas, mientras que las repeticiones se enviaron a cuarentena_ventas etiquetadas con el motivo 'Venta_id duplicado'.
# MAGIC
# MAGIC 2. ¿Qué hicieron con un monto que venía en texto?
# MAGIC
# MAGIC Se estandarizó reemplazando comas por puntos y convirtiéndolo con try_cast(... as double). Los importes con coma decimal válidos (como "12,50") ingresaron como números a oro_fact_ventas, mientras que las cadenas no numéricas (como "N/D" o "pendiente") se derivaron a cuarentena_ventas con el motivo 'Monto no numérico o inválido'.
# MAGIC
# MAGIC 3. ¿Por qué una tienda se quedó fuera?
# MAGIC
# MAGIC Las reglas de negocio estipulan que únicamente se admiten transacciones de las tiendas 1, 2 y 3. Las transacciones de cualquier otra tienda (4, 5, 6, 7, 77, 88, 99, etc.) se excluyeron del hecho y se registraron en cuarentena_ventas bajo el motivo 'Tienda no permitida'.
# MAGIC
# MAGIC 4. ¿Dónde quedó la columna que no estaba en el contrato?
# MAGIC
# MAGIC La columna canal no forma parte del contrato de 7 columnas definido para oro_fact_ventas. Fue omitida explícitamente en el SELECT de la tabla de hechos, por lo que solo permanece disponible en bronce_ventas y en cuarentena_ventas para trazabilidad.
# MAGIC
# MAGIC 5. Si corren el notebook otra vez, ¿el hecho crece?
# MAGIC
# MAGIC No, el proceso es totalmente idempotente. Al utilizar la sintaxis CREATE OR REPLACE TEMP VIEW, cada reejecución sobrescribe la vista previa a partir del origen de datos, garantizando que no se dupliquen filas ni aumente el conteo final.
# MAGIC
# MAGIC El que solo pega la consulta y no puede explicar una de estas, no termino el laboratorio

# COMMAND ----------

# MAGIC %md
# MAGIC