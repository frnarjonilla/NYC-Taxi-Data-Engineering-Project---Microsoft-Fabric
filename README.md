🚕 NYC Taxi Data Engineering Project - Microsoft Fabric
Este proyecto implementa una solución de Modern Data Stack utilizando Microsoft Fabric. Se ha diseñado un pipeline de datos completo siguiendo la Arquitectura Medallion (Bronze -> Silver -> Gold) para procesar millones de registros de viajes de taxis de la ciudad de Nueva York.

🏗️ Arquitectura del Proyecto
El flujo de datos se divide en tres capas lógicas para asegurar la calidad y el rendimiento:

Capa Bronze (Lakehouse): Ingesta de archivos CSV crudos desde OneLake.

Capa Silver (Warehouse): Limpieza, tipado de datos (Decimal, Datetime2) y eliminación de duplicados mediante scripts SQL dinámicos.

Capa Gold (Modelo Semántico): Creación de un modelo en estrella (Star Schema) optimizado para análisis con tecnología Direct Lake.

🛠️ Desafíos Técnicos y Soluciones
1. Ingesta Dinámica y Automatizada
Se desarrolló un Pipeline de Data Factory que utiliza parámetros para procesar diferentes meses.

Problema: Los archivos CSV suelen contener registros "sucios" de otros meses o años (ej. registros de 2026 en archivos de 2019).

Solución: Implementación de lógica SQL avanzada con CHARINDEX y SUBSTRING para extraer el mes y año directamente del nombre del archivo, asegurando que solo los datos correctos lleguen a la capa Silver.

SQL
-- Ejemplo de la lógica de filtrado dinámico
WHERE YEAR(Fecha_Recogida) = 2019
  AND MONTH(Fecha_Recogida) = CAST(SUBSTRING(Parametro_Archivo, CHARINDEX('2019-', Parametro_Archivo) + 5, 2) AS INT)
  
  2. Integridad de Datos
Para evitar la duplicidad en cargas incrementales, se implementó una estrategia de Upsert/Check mediante la cláusula NOT EXISTS, comparando la clave compuesta de Vendedor, Fecha y Monto Total.

3. Modelo Semántico de Alto Rendimiento
En lugar de usar Import o DirectQuery tradicional, el proyecto utiliza Direct Lake.

Esta tecnología permite que Power BI lea directamente los archivos Delta/Parquet de OneLake, ofreciendo la velocidad de una base de datos en memoria con la frescura de datos en tiempo real.

📊 Modelo de Datos (Star Schema)
El modelo semántico relaciona la tabla de hechos (Silver_Viajes_Taxis) con cuatro dimensiones clave:

Calendario: Análisis por día, mes, año y día de la semana.

Zonas (Ubicación): Mapeo de IDs a nombres de distritos y barrios de NY.

Tipos de Pago: Clasificación de transacciones (Crédito, Efectivo, etc.).

Vendedores: Identificación de las empresas de taxis.

🚀 Tecnologías Utilizadas
Microsoft Fabric: Ecosistema completo.

Data Factory: Pipelines de orquestación.

Synapse Data Warehouse: Procesamiento T-SQL escalable.

OneLake: Almacenamiento unificado en formato Delta/Parquet.

Power BI: Visualización avanzada de datos.

Cómo usar este repositorio
En la carpeta /sql encontrarás los scripts para crear la estructura de las tablas.

En /pipelines se encuentra la lógica de orquestación.

Revisa la carpeta /docs para ver capturas del modelo de datos y el dashboard final.
