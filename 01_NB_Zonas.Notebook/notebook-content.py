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

# MAGIC %%spark
# MAGIC // Importamos las funciones necesarias
# MAGIC // Para usar el símbolo $, necesitamos importar los implicits
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import spark.implicits._
# MAGIC 
# MAGIC // 1. Leer el csv
# MAGIC // En Scala usamos val para definir variables inmutables
# MAGIC val dfZonasRaw = spark.read
# MAGIC     .format("csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("inferSchema", "true")
# MAGIC     .load("Files/taxi+_zone_lookup.csv")
# MAGIC 
# MAGIC dfZonasRaw.printSchema()
# MAGIC 
# MAGIC val dfZonasSilver = dfZonasRaw.select(
# MAGIC     $"LocationID".as("ID_Ubicacion"),
# MAGIC     $"Borough".as("Distrito"),
# MAGIC     $"Zone".as("Barrio"),
# MAGIC     $"service_zone".as("Zona_Servicio")
# MAGIC ).filter($"LocationID".isNotNull)
# MAGIC 
# MAGIC //Guardar en el Lakehouse
# MAGIC dfZonasSilver.write
# MAGIC     .format("delta")
# MAGIC     .mode("overwrite")
# MAGIC     .saveAsTable("sc.Silver_Zonas")

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }
