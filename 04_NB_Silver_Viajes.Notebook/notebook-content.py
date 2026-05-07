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
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import spark.implicits._
# MAGIC 
# MAGIC val dfViajesSilver = spark.read.table("sc.bronze_viajes").select(
# MAGIC     $"VendorID".cast(IntegerType).as("ID_Vendedor"),
# MAGIC     $"tpep_pickup_datetime".as("Fecha_Inicio"),
# MAGIC     $"tpep_dropoff_datetime".as("Fecha_Fin"),
# MAGIC     $"passenger_count".cast(IntegerType).as("Num_Pasajeros"),
# MAGIC     $"trip_distance".cast(DoubleType).as("Distancia_Viaje"),
# MAGIC     $"RatecodeID".cast(IntegerType).as("ID_Tarifa"),
# MAGIC     $"PULocationID".cast(IntegerType).as("ID_Origen"),
# MAGIC     $"DOLocationID".cast(IntegerType).as("ID_Destino"),
# MAGIC     $"payment_type".cast(IntegerType).as("ID_Tipo_Pago"),
# MAGIC     $"fare_amount".as("Tarifa_Base"),
# MAGIC     $"tip_amount".as("Propina"),
# MAGIC     $"total_amount".as("Total_Pagado")
# MAGIC )
# MAGIC 
# MAGIC // 3. Cálculo de duración y filtros
# MAGIC // En Scala, la función unix_timestamp devuelve una columna directamente
# MAGIC val dfViajesFinal = dfViajesSilver
# MAGIC     .withColumn(
# MAGIC         "Duracion_Minutos",
# MAGIC         (unix_timestamp($"Fecha_Fin") - unix_timestamp($"Fecha_Inicio")) / 60
# MAGIC     )
# MAGIC     .filter($"Distancia_Viaje" > 0 && $"Duracion_Minutos" > 0)
# MAGIC 
# MAGIC //Guardamos en el Lakehouse
# MAGIC dfViajesFinal.write
# MAGIC     .format("delta")
# MAGIC     .mode("overwrite")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .saveAsTable("sc.Silver_Viajes")
# MAGIC 
# MAGIC println("Tabla Silver_Viajes creada con éxito")

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }
