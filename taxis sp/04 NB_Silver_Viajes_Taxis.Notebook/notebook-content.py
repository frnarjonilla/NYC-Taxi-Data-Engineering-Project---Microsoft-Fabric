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

df_bronze = spark.read.table("sp.bronze_viajes")
df_viajes_silver = df_bronze.select(
    F.col("VendorID").cast(IntegerType()).alias("ID_Vendedor"),
    F.col("tpep_pickup_datetime").alias("Fecha_Inicio"),
    F.col("tpep_dropoff_datetime").alias("Fecha_Fin"),
    F.col("passenger_count").cast(IntegerType()).alias("Num_Pasajeros"),
    F.col("trip_distance").cast(DoubleType()).alias("Distancia_Viaje"),
    F.col("RatecodeID").cast(IntegerType()).alias("ID_Tarifa"),
    F.col("PULocationID").cast(IntegerType()).alias("ID_Origen"),
    F.col("DOLocationID").cast(IntegerType()).alias("ID_Destino"),
    F.col("payment_type").cast(IntegerType()).alias("ID_Tipo_Pago"),
    F.col("fare_amount").alias("Tarifa_Base"),
    F.col("tip_amount").alias("Propina"),
    F.col("total_amount").alias("Total_Pagado")
)

#Cálculo de la duracion en minutos
# Restamos fechas (da seguntos) y dividimos por 60
df_viajes_silver = df_viajes_silver.withColumn(
    "Duracion_Minutos",
    (F.unix_timestamp("Fecha_Fin") - F.unix_timestamp("Fecha_Inicio")) / 60
)

# Filtro de calidad: Solo viajes con distancia y duración positiva
df_viajes_silver = df_viajes_silver.filter(
    (F.col("Distancia_Viaje") > 0) & (F.col("Duracion_Minutos") > 0)
)

# Guardamos en el Lakehouse
df_viajes_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("sp.Silver_Viajes")

print("Tabla Silver_viajes creada con éxito")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
