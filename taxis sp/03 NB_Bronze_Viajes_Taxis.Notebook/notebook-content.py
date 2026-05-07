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
from pyspark.sql.types import DoubleType, IntegerType

#Leemos el CSV. Spark iferirá que los números son números y las fechas son string por ahora
df_viajes = spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True")\
    .load("Files/yellow_tripdata_2019-01.csv")

# Guardamos la tabla en bronze en el lakehouse
df_viajes.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("sp.Bronze_Viajes")

print("Tabla Bronze_Viajes creada con éxito")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
