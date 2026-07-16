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

import datetime

# 1. Intentar obtener el ID del Pipeline, si falla, crear uno manual
try:
    id_run = mssparkutils.runtime.context().get('currentPipelineRunId', 'Manual_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
except:
    id_run = "Ejecucion_Desconocida_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

paso = "SILVER"

# 2. Forzar el registro (Añadimos un print para depurar)
print(f"Intentando registrar inicio de {paso} con ID: {id_run}")

spark.sql(f"""
    INSERT INTO Control_Ejecuciones (ID_Ejecucion, Paso_Pipeline, Fecha_Inicio, Estado)
    VALUES ('{id_run}', '{paso}', from_utc_timestamp(now(), 'Europe/Madrid'), 'PROCESANDO')
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, unix_timestamp

try:
    # --- PARTE 1: LIMPIEZA DE TAXIS (Tu Captura 58) ---
    print("Limpiando datos de Taxis...")
    df_bronze = spark.read.table("BR_taxis_STAGING")
    df_clean = df_bronze.filter((col("trip_distance") > 0) & (col("passenger_count") > 0) & (col("fare_amount") > 0))
    
    # Calcular duración en minutos
    df_with_duration = df_clean.withColumn(
    "Duracion_Minutos", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
)
    
    df_final = df_with_duration.select(
        col("VendorID").alias("ID_Proveedor"),
        col("tpep_pickup_datetime").alias("Fecha_Recogida"),
        col("tpep_dropoff_datetime").alias("Fecha_Entrega"),
        col("passenger_count").alias("Num_Pasajeros"),
        col("trip_distance").alias("Distancia_Viaje"),
        col("RatecodeID").alias("ID_Tarifa"),
        col("PULocationID").alias("ID_Zona_Origen"),
        col("DOLocationID").alias("ID_Zona_Destino"),
        col("payment_type").alias("Tipo_Pago"),
        col("fare_amount").alias("Tarifa_Base"),
        col("extra").alias("Extra_Cargos"),
        col("mta_tax").alias("Impuesto_MTA"),
        col("tip_amount").alias("Propina"),
        col("tolls_amount").alias("Peajes"),
        col("total_amount").alias("Total_Pagado"),
        col("Duracion_Minutos")
    )
    df_final.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("SL_taxis_STAGING")

    # --- PARTE 2: LIMPIEZA DE ZONAS ---
    print("Traduciendo zonas...")
    df_bronze = spark.read.table("br_zonas_taxis")
    silver_zonas = df_bronze.select(
        col("LocationID").alias("ID_Zona"),
        col("Borough").alias("Distrito"),
        col("Zone").alias("Barrio")
    )
    silver_zonas.write.format("delta").mode("overwrite").saveAsTable("SL_zonas_taxis")

    # --- PARTE 3: TABLA DE PAGOS ---
    print("Creando tabla de tipos de pago...")
    pagos_data = [
        (1, "Tarjeta de Crédito"), (2, "Efectivo"), (3, "Sin Cargo"),
        (4, "Disputa"), (5, "Desconocido"), (6, "Anulado")
    ]
    df_pagos = spark.createDataFrame(pagos_data, ["ID_Pago", "Tipo_Pago_Desc"])
    df_pagos.write.format("delta").mode("overwrite").saveAsTable("dim_tipos_pago")

    # --- REGISTRO DE EXITO (OK) ---
    spark.sql(f"""
    UPDATE Control_Ejecuciones 
    SET Fecha_Fin = from_utc_timestamp(now(), 'Europe/Madrid'), Estado = 'EXITO', Error_Mensaje = 'Todo OK'
    WHERE ID_Ejecucion = '{id_run}' AND Paso_Pipeline = '{paso}'
""")
    print(f">>> Auditoría: {paso} finalizado con ÉXITO.")

except Exception as e:
    # --- REGISTRO DE ERROR (KO) ---
    error_msg = str(e).replace("'", " ")
    spark.sql(f"""
    UPDATE Control_Ejecuciones 
    SET Fecha_Fin = from_utc_timestamp(now(), 'Europe/Madrid'), Estado = 'FALLO', Error_Mensaje = '{error_msg[:500]}'
    WHERE ID_Ejecucion = '{id_run}' AND Paso_Pipeline = '{paso}'
""")
    print(f">>> Auditoría: ERROR en Silver.")
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
