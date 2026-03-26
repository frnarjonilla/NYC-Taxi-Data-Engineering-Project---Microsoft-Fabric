# -------------------------------------------------------------------------
# INGESTA DE CAPA BRONCE: CATÁLOGO DE ZONAS (TAXI ZONES)
# -------------------------------------------------------------------------
# Descripción: 
# Este Notebook realiza la lectura del archivo maestro de zonas de NYC 
# desde el formato CSV y lo persiste en formato Delta dentro del Lakehouse.
#
# Hitos técnicos:
# 1. Uso de PySpark para lectura con inferencia de esquema automática.
# 2. Persistencia en formato Delta (estándar abierto de OneLake).
# 3. Organización lógica mediante esquemas (Carpeta 'inc' en Bronze).
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession

# 1. Lectura del archivo CSV desde el almacenamiento de OneLake (Files)
# Usamos inferSchema para que Spark detecte automáticamente tipos de datos
df_raw_zonas = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/taxi+_zone_lookup.csv")

# 2. Definición del destino en la arquitectura Medallion
# En Microsoft Fabric, usamos la nomenclatura 'esquema.nombre_tabla'
# para organizar las tablas dentro del Lakehouse.
nombre_tabla = "inc.Bronze_Zonas"

# 3. Escritura en formato Delta
# El modo 'overwrite' asegura que la dimensión se actualice completamente
# manteniendo las ventajas de ACID de las tablas Delta.
df_raw_zonas.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(nombre_tabla)

print(f"¡Ingesta completada! Tabla guardada exitosamente en la capa Bronze como: {nombre_tabla}")

# 4. Vista previa de los datos cargados
display(df_raw_zonas.limit(5))
