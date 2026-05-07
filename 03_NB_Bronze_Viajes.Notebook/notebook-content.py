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
# MAGIC import org.apache.spark.sql.types._
# MAGIC import spark.implicits._
# MAGIC 
# MAGIC // 1. Leer el CSV
# MAGIC val dfViajesRaw = spark.read
# MAGIC .format("csv")
# MAGIC .option("header", "true")
# MAGIC .option("inferSchema", "true")
# MAGIC .load("Files/yellow_tripdata_2019-01.csv")
# MAGIC 
# MAGIC // Guardamos en el Lakehouse
# MAGIC dfViajesRaw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sc.Bronze_Viajes")
# MAGIC 
# MAGIC print("Tabla Creada con éxito")

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }
