# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a129bb89-7eef-4cf2-98e9-e7eb2c055c27",
# META       "default_lakehouse_name": "Lakehouse_Taxis_NY",
# META       "default_lakehouse_workspace_id": "5380f59f-5ecb-43d3-ac2d-b87dd26ea6ec",
# META       "known_lakehouses": [
# META         {
# META           "id": "a129bb89-7eef-4cf2-98e9-e7eb2c055c27"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, explode, sequence, to_timestamp, to_date, year, month, date_format, dayofmonth, dayofweek, hour

# 1. Definir el rango de fechas (2019 y 2020)
# Usamos formato Timestamp para que incluya información de horas
inicio = '2019-01-01 00:00:00'
fin = '2020-12-31 23:59:59'

# 2. Generar la secuencia de HORAS
# 'interval 1 hour' creará 24 filas por cada día
df_cal = spark.sql(f"SELECT sequence(to_timestamp('{inicio}'), to_timestamp('{fin}'), interval 1 hour) as fecha") \
    .withColumn("fecha", explode(col("fecha")))

# 3. Extraer todos los atributos necesarios para Power BI
df_calendario = df_cal.select(
    col("fecha").alias("Fecha_Full"),                 # Timestamp completo (2019-01-01 14:00:00)
    to_date(col("fecha")).alias("Fecha"),            # Solo fecha para relaciones (2019-01-01)
    year(col("fecha")).alias("Anio"),
    month(col("fecha")).alias("Mes_Numero"),
    date_format(col("fecha"), "MMMM").alias("Mes_Nombre"),
    dayofmonth(col("fecha")).alias("Dia"),
    dayofweek(col("fecha")).alias("Dia_Semana_Numero"),
    date_format(col("fecha"), "EEEE").alias("Dia_Semana_Nombre"),
    hour(col("fecha")).alias("Hora_Entera")          # <--- ¡Aquí ya no saldrán ceros!
)

# 4. Guardar en el Lakehouse (Capa Silver)
# Importante: Usamos overwriteSchema para que Delta acepte la nueva columna 'Hora_Entera'
df_calendario.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_calendario")

print("✅ Tabla 'dim_calendario' generada con éxito.")
print(f"Total de registros (horas): {df_calendario.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
