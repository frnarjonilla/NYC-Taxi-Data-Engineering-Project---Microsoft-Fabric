El Pipeline de Data Factory actúa como el director de orquesta. Realiza las siguientes tareas:

Parámetro de Entrada: Recibe el nombre del archivo (ej. yellow_tripdata_2019-01.csv).

Carga Bronze: Mueve el archivo del Data Lake a una tabla Delta temporal.

Transformación Silver: Ejecuta el script SQL dinámico que limpia, tipa y elimina duplicados usando el parámetro del nombre del archivo.

Control de Errores: Si la carga falla, el pipeline se detiene para evitar datos inconsistentes.
