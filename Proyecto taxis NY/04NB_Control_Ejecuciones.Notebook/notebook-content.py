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

# Definimos el esquema de la tabla de auditoría
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("ID_Ejecucion", StringType(), True),
    StructField("Nombre_Archivo", StringType(), True),
    StructField("Paso_Pipeline", StringType(), True),
    StructField("Fecha_Inicio", TimestampType(), True),
    StructField("Fecha_Fin", TimestampType(), True),
    StructField("Estado", StringType(), True),
    StructField("Error_Mensaje", StringType(), True)
])

# Creamos un DataFrame vacío con ese esquema
df_vacio = spark.createDataFrame([], schema)

# Guardamos el DataFrame como una tabla Delta en el Lakehouse
# Esto creará la tabla físicamente y será editable por Spark
df_vacio.write.format("delta").mode("overwrite").saveAsTable("Control_Ejecuciones")

print("¡Tabla 'Control_Ejecuciones' creada con éxito en el Lakehouse!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
