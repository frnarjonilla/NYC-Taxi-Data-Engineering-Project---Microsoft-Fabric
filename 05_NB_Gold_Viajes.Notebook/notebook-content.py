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
# MAGIC import spark.implicits._
# MAGIC 
# MAGIC // 1. Cargamos las tablas Silver ya creadas
# MAGIC val dfViajes = spark.read.table("sc.silver_viajes")
# MAGIC val dfZonas = spark.read.table("sc.silver_zonas")
# MAGIC 
# MAGIC // 2. Hacemos del JOIN
# MAGIC // Unimos el ID_Origen del viaje con el ID_Ubicacion de la zona
# MAGIC val dfGold = dfViajes.join(
# MAGIC     dfZonas.select($"ID_Ubicacion", $"Barrio".as("Barrio_Origen"), $"Distrito".as("Distrito_Origen")),
# MAGIC     $"ID_Origen" === $"ID_Ubicacion",
# MAGIC     "left"
# MAGIC )
# MAGIC 
# MAGIC // 3. Guardamos el resultado en una tabla GOLD
# MAGIC dfGold.write.format("delta").mode("overwrite").saveAsTable("sc.gold_viajes")
# MAGIC 
# MAGIC println("Capa GOLD creada con éxito")
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import spark.implicits._
# MAGIC 
# MAGIC // 1. Definimos el esquema manualmente (Case Class falla el Encoder)
# MAGIC val esquemaViaje = StructType(Array(
# MAGIC     StructField("ID_Vendedor", IntegerType, true),
# MAGIC     StructField("Distancia_Viaje", DoubleType, true),
# MAGIC     StructField("Total_Pagado", DoubleType, true),
# MAGIC     StructField("Barrio_Origen", StringType, true)
# MAGIC ))
# MAGIC 
# MAGIC // 2. Leemos la tabla y forzamos el esquema
# MAGIC // Al usar select sobre las columnas específicas, garantizamos el tipado
# MAGIC val dfGold = spark.read.table("sc.gold_viajes")
# MAGIC     .select(
# MAGIC         $"ID_Vendedor".cast(IntegerType),
# MAGIC         $"Distancia_Viaje".cast(DoubleType),
# MAGIC         $"Total_Pagado".cast(DoubleType),
# MAGIC         $"Barrio_Origen".cast(StringType),
# MAGIC         $"Fecha_Inicio",
# MAGIC         $"Fecha_Fin",
# MAGIC         $"ID_Origen"
# MAGIC     )
# MAGIC 
# MAGIC // 3. Aplicamos el filtro usando la sintaxis de columna
# MAGIC val viajesLargos = dfGold.filter($"Distancia_Viaje" > 20)
# MAGIC 
# MAGIC println(s"Tienes ${viajesLargos.count()} viajes de larga distancia procesados")
# MAGIC display(viajesLargos)

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC 
# MAGIC //Queremos saber, dentro de cada Barrio, cuál ha sido el viaje más caro
# MAGIC // Creamos la ventana, particionamos por Barrio_Origen y ordenamos por Total_Pagado
# MAGIC val ventanaPrecio = Window.partitionBy("Barrio_Origen").orderBy(desc("Total_Pagado"))
# MAGIC 
# MAGIC val dfPrecio = dfGold.withColumn("Top_Viaje", row_number().over(ventanaPrecio))
# MAGIC 
# MAGIC val resultadoFinal = dfPrecio.filter(col("Top_Viaje") === 1)
# MAGIC 
# MAGIC resultadoFinal.show()


# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC // Queremos ver cómo se van acumulando los ingresos en cada barrio a lo largo del día
# MAGIC 
# MAGIC val ventanaIngresos = Window.partitionBy("Barrio_Origen").orderBy("Fecha_Inicio")
# MAGIC 
# MAGIC val dfIngreso = dfGold.withColumn("Total_Ingresos", sum(col("Total_Pagado")).over(ventanaIngresos))
# MAGIC 
# MAGIC dfIngreso.show()

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC // Cuánto tiempo pasa un taxi "libre" entre un viaje y otro
# MAGIC 
# MAGIC val dfSoloUnTaxi = dfGold.filter($"ID_Vendedor" === 1 && $"ID_Origen" === 140)
# MAGIC 
# MAGIC val ventanaLibre = Window.partitionBy("ID_Vendedor", "ID_Origen").orderBy("Fecha_Inicio")
# MAGIC 
# MAGIC val dfLibre = dfSoloUnTaxi.withColumn("Prox_Viaje", lead("Fecha_Inicio", 1).over(ventanaLibre))
# MAGIC                     .withColumn("Minutos_Libre", (unix_timestamp(col("Prox_Viaje")) - unix_timestamp(col("Fecha_Fin"))) / 60)
# MAGIC 
# MAGIC dfLibre.show()

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC // ¿Qué barrios son los más rentables por cada kilómetro recorrido?
# MAGIC 
# MAGIC val ventanaBarrio = Window.partitionBy("Barrio_Origen")
# MAGIC 
# MAGIC val Barrio = dfGold.withColumn("Ratio_Rentabilidad", col("Total_Pagado") / col("Distancia_Viaje"))
# MAGIC                     .withColumn("Prom_Rent_Barrio", avg("Ratio_Rentabilidad").over(ventanaBarrio))
# MAGIC                     .withColumn("DifvsMedia", col("Ratio_Rentabilidad") - col("Prom_Rent_Barrio"))
# MAGIC 
# MAGIC val BarrioLimpio = Barrio
# MAGIC     .withColumn("Ratio_Rentabilidad", round(col("Ratio_Rentabilidad"), 2))
# MAGIC     .withColumn("Prom_Rent_Barrio", round(col("Prom_Rent_Barrio"), 2))
# MAGIC     .withColumn("DifvsMedia", round(col("DifvsMedia"), 2))
# MAGIC 
# MAGIC BarrioLimpio.select("Barrio_Origen", "Ratio_Rentabilidad", "Prom_Rent_Barrio", "DifvsMedia").show(10)

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }
