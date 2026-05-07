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
# MAGIC 
# MAGIC //Definimos los datos como una secuencia de tuplas
# MAGIC val datosPagos = Seq(
# MAGIC     (1, "Tarjeta de Crédito"),
# MAGIC     (2, "Efectivo"),
# MAGIC     (3, "Sin Cargo"),
# MAGIC     (4, "Disputa"),
# MAGIC     (5, "Desconocido"),
# MAGIC     (6, "Viaje Anulado")
# MAGIC )
# MAGIC 
# MAGIC // Convertimos a DataFrame pasándole los nombres de las columnas
# MAGIC val dfPagos = datosPagos.toDF("ID_Tipo_Pago", "Descripcion_Pago")
# MAGIC 
# MAGIC // Guardamos en el Lakehouse
# MAGIC dfPagos.write.format("delta").mode("overwrite").saveAsTable("sc.Silver_Tipos_Pago")
# MAGIC 
# MAGIC print("Tabla de Tipos de Pago creada con éxito")
# MAGIC display(dfPagos)

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }
