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

from pyspark.sql import functions as F

# 1. Leer el CSV
# "header" True porque el archivo tiene nombres de columnas
# "inferSchema" True para que detecte si es numero o texto automaticamente
df_zonas_bronze = spark.read.format("csv") \
    .option("header", "True") \
    .option("inferschema", "True") \
    .load("Files/taxi+_zone_lookup.csv")

# 2. Ver qué columnas trajo y qué tipo de datos tienen
df_zonas_bronze.printSchema()

# 3. Mostrar las primeras 5 filas
display(df_zonas_bronze.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aplicamos las transformaciones
df_zonas_silver = df_zonas_bronze.select(
    F.col("LocationID").alias("ID_Ubicacion"),
    F.col("Borough").alias("Distrito"),
    F.col("zone").alias("Barrio"),
    F.col("service_zone").alias("Zona_Servicio")
).filter(F.col("LocationID").isNotNull())

# Guardamos la tabla Delta en el Lakehouse
df_zonas_silver.write.format("delta").mode("overwrite").saveAsTable("sp.Dim_Zonas")

print("Tabla Dim_Zonas creada con éxito")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
