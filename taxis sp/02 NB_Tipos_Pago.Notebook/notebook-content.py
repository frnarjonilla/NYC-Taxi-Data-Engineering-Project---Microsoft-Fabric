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

## Creamos la tabla Tipos_Pago
# Definimos los datos
datos_pagos = [
    (1, 'Tarjeta de Crédito'),
    (2, 'Efectivo'),
    (3, 'Sin Cargo'),
    (4, 'Disputa'),
    (5, 'Desconocido'),
    (6, 'Viaje Anulado')
]

#Definimos los nombres de las columnas
columnas = ["ID_Tipo_Pago", "Descripcion_Pago"]

# Creamos el DataFrame
df_pagos = spark.createDataFrame(datos_pagos, columnas)

#Guardamos
df_pagos.write.format("delta").mode("overwrite").saveAsTable("sp.Dim_Tipos_Pago")

print("Tabla Dim_Tipos_Pago creada con éxito")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
