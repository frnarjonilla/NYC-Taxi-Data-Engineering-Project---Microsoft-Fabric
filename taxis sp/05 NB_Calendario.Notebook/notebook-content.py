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

# Creamos una secuencia de fechas (del 1 de enero al 31 de diciembre)
df_calendario = spark.range(1).select(
    F.explode(F.sequence(F.to_date(F.lit("2019-01-01")), F.to_date(F.lit("2019-12-31")), F.expr("interval 1 day")))
    .alias("Fecha")
)

# Extraemos todos los atributos que necesitaremos para filtrar
df_calendario = df_calendario.select(
    F.col("Fecha"),
    F.year("Fecha").alias("Anio"),
    F.month("Fecha").alias("Mes"),
    F.dayofmonth("Fecha").alias("Dia"),
    F.quarter("Fecha").alias("Trimestre"),
    F.dayofweek("Fecha").alias("Dia_semana_num"), # 1=Domingo, 7=Sábado
    F.date_format("Fecha", "EEEE").alias("Nombre_Dia"), # Lunes, Martes...
    F.date_format("Fecha", "MMMM").alias("Nombre_Mes"), # Enero, Febrero...
    #Creamos una bandera para saber si es fin de semana
    F.when(F.dayofweek("Fecha").isin(1, 7), "Fin de Semana").otherwise("Dia_Laboral").alias("Tipo_Dia")
)

# Guardamos en el Lakehouse
df_calendario.write.format("delta").mode("overwrite").saveAsTable("sp.dim_calendario")

print("Tabla Calendario creada con exito")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
