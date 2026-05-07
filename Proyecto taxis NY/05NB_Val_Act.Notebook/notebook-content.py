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

#Solo se ejecuta si TODO lo anterior fue bien
# --- 1. ACTUALIZACIÓN DE TABLA BRONZE ---
print("Actualizando Bronze Final...")
# Creamos la tabla si la borraste por error
spark.sql("""
    CREATE TABLE IF NOT EXISTS br_taxis 
    USING DELTA 
    AS SELECT * FROM br_taxis_staging WHERE 1=0
""")
# Insertamos los datos frescos
spark.sql("INSERT OVERWRITE TABLE br_taxis SELECT * FROM br_taxis_staging")


# --- 2. ACTUALIZACIÓN DE TABLA SILVER ---
print("Actualizando Silver Final...")
# Creamos la tabla si no existe
spark.sql("""
    CREATE TABLE IF NOT EXISTS sl_taxis 
    USING DELTA 
    AS SELECT * FROM sl_taxis_staging WHERE 1=0
""")
# Insertamos los datos limpios
spark.sql("INSERT OVERWRITE TABLE sl_taxis SELECT * FROM sl_taxis_staging")


# --- 3. LIMPIEZA DE ESCENARIO (STAGING) ---
# Solo llegamos aquí si los INSERT de arriba funcionaron
print("Limpiando tablas temporales de Staging...")
spark.sql("DROP TABLE IF EXISTS br_taxis_staging")
spark.sql("DROP TABLE IF EXISTS sl_taxis_staging")

print("✅ Pipeline finalizado con éxito: Datos publicados y Staging eliminado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
