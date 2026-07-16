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

# 1. Obtener el ID de ejecución del Pipeline de Fabric
try:
    id_run = mssparkutils.runtime.context()['currentPipelineRunId']
except:
    id_run = "Ejecucion_Manual_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

paso = "BRONZE"

# 2. Registrar el inicio en la tabla de auditoría
spark.sql(f"""
    INSERT INTO Control_Ejecuciones (ID_Ejecucion, Paso_Pipeline, Fecha_Inicio, Estado)
    VALUES ('{id_run}', '{paso}', from_utc_timestamp(now(), 'Europe/Madrid'), 'PROCESANDO')
""")

print(f">>> Auditoría: Iniciando {paso} (ID: {id_run})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # --- PARTE 1: TABLA GRANDE Y PARQUET (Tu Celda 1 de la captura) ---
    print("Iniciando procesamiento de datos de viajes...")
    input_path = "Files/yellow_tripdata_*.csv"
    output_path = "Files/parquet_taxis_nyc/"

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_path)

    # Guardar en carpeta Files como Parquet
    df.write.format("parquet").mode("overwrite").save(output_path)
    
    # Guardar como Tabla Delta
    # Guardamos en una tabla que nadie más usa todavía
    df.write.format("delta").mode("overwrite").saveAsTable("BR_taxis_STAGING")
    print("¡Conversión de viajes completada con éxito!")


    # --- PARTE 2: TABLA DE ZONAS (Tu Celda 2 de la captura) ---
    print("Iniciando carga de tabla de zonas...")
    df_zonas_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("Files/taxi+_zone_lookup.csv")

    # Guardamos en una tabla que nadie más usa todavía
    df_zonas_raw.write.format("delta").mode("overwrite").saveAsTable("BR_zonas_taxis")
    print("Capa Bronze finalizada: tabla 'BR_zonas_taxis' cargada con éxito")


    # --- SI TODO SALIÓ BIEN: REGISTRO DE EXITO (OK) ---
    spark.sql(f"""
    UPDATE Control_Ejecuciones 
    SET Fecha_Fin = from_utc_timestamp(now(), 'Europe/Madrid'), Estado = 'EXITO', Error_Mensaje = 'Todo OK'
    WHERE ID_Ejecucion = '{id_run}' AND Paso_Pipeline = '{paso}'
""")
    print(f">>> Auditoría: {paso} finalizado con ÉXITO en la tabla de control.")

except Exception as e:
    # --- SI ALGO FALLÓ (KO): REGISTRO DE ERROR ---
    error_msg = str(e).replace("'", " ")
    spark.sql(f"""
    UPDATE Control_Ejecuciones 
    SET Fecha_Fin = from_utc_timestamp(now(), 'Europe/Madrid'), Estado = 'FALLO', Error_Mensaje = '{error_msg[:500]}'
    WHERE ID_Ejecucion = '{id_run}' AND Paso_Pipeline = '{paso}'
""")
    print(f">>> Auditoría: ERROR detectado en {paso}. Se ha registrado en la tabla de control.")
    
    # Lanzamos el error para que el Pipeline de Fabric se ponga en rojo
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
