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

df_raw_zonas = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/taxi+_zone_lookup.csv")

# 2. Definir la ruta de la carpeta 'inc' dentro de Tablas
# En Fabric, las tablas suelen guardarse en 'Tables/nombre_tabla'
# Si quieres que esté dentro de una subcarpeta lógica:
nombre_tabla = "inc.Bronze_Zonas"

# 3. Guardar la tabla
# Al usar el punto (inc.Silver_Zonas), Spark lo interpreta como esquema.tabla
df_raw_zonas.write.format("delta").mode("overwrite").saveAsTable(nombre_tabla)

print(f"¡Tabla guardada exitosamente en la carpeta 'inc' como {nombre_tabla}!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
