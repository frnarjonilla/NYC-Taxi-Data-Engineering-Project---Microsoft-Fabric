# 🚕 NYC Taxi Data Engineering Project - Microsoft Fabric

Este proyecto implementa una solución de **Modern Data Stack** utilizando **Microsoft Fabric**. Se ha diseñado un pipeline de datos completo siguiendo la **Arquitectura Medallion** (Bronze -> Silver -> Gold) para procesar millones de registros de viajes de taxis de la ciudad de Nueva York.

---

## 🏗️ Arquitectura del Proyecto
El flujo de datos se divide en tres capas lógicas para asegurar la calidad y el rendimiento:

* **Capa Bronze (Lakehouse):** Ingesta de archivos CSV crudos desde OneLake.
* **Capa Silver (Warehouse):** Limpieza, tipado de datos (Decimal, Datetime2) y eliminación de duplicados mediante scripts SQL dinámicos.
* **Capa Gold (Modelo Semántico):** Creación de un modelo en estrella (*Star Schema*) optimizado para análisis con tecnología **Direct Lake**.

---

## 🛠️ Desafíos Técnicos y Soluciones

### 1. Ingesta Dinámica y Automatizada
Se desarrolló un **Pipeline de Data Factory** que utiliza parámetros para procesar diferentes meses de forma automática.

* **Problema:** Los archivos CSV suelen contener registros "sucios" de otros meses o años (ej. registros de otros años en archivos de 2019).
* **Solución:** Implementación de lógica SQL avanzada con `CHARINDEX` y `SUBSTRING` para extraer el mes y año directamente del nombre del archivo, asegurando que solo los datos correctos lleguen a la capa Silver.

```sql
-- Ejemplo de la lógica de filtrado dinámico
WHERE YEAR(tpep_pickup_datetime) = 2019
  AND MONTH(tpep_pickup_datetime) = CAST(SUBSTRING('@{pipeline().parameters.Mes_Carga}', 
      CHARINDEX('2019-', '@{pipeline().parameters.Mes_Carga}') + 5, 2) AS INT)
```

### 2. Integridad de Datos
Para evitar la duplicidad en cargas incrementales, se implementó una estrategia de **Upsert/Check** mediante la cláusula `NOT EXISTS`. Se diseñó una validación basada en una clave compuesta:
* **ID_Vendedor**
* **Fecha_recogida**
* **Monto_Total**

Esta lógica asegura que, aunque el proceso se ejecute varias veces para el mismo archivo, el Data Warehouse permanezca limpio y sin registros duplicados.

### 3. Modelo Semántico de Alto Rendimiento
En lugar de usar los métodos tradicionales de *Import* o *DirectQuery*, este proyecto aprovecha la tecnología **Direct Lake**. 

> **Nota técnica:** Esta arquitectura permite que Power BI acceda directamente a los archivos **Delta/Parquet** en OneLake. El resultado es el rendimiento de una base de datos *in-memory* con la ventaja de no tener que programar actualizaciones de datos (refresh), ya que lee directamente del almacenamiento unificado.

---

## 📊 Modelo de Datos (Star Schema)
El modelo semántico sigue un diseño de **Esquema en Estrella**, optimizado para el filtrado y la agregación de grandes volúmenes de datos. La tabla de hechos (`Silver_Viajes_Taxis`) se relaciona con cuatro dimensiones clave:

* **Calendario:** Permite análisis temporales por día, mes, año y día de la semana.
* **Zonas (Ubicación):** Mapeo de IDs numéricos a nombres reales de distritos y barrios de Nueva York.
* **Tipos de Pago:** Clasificación descriptiva de las transacciones (Tarjeta de Crédito, Efectivo, Disputa, etc.).
* **Vendedores:** Identificación de las empresas proveedoras del servicio de taxi.

---

## 🚀 Tecnologías Utilizadas
* **PySpark (Notebooks):** Ingesta inicial de catálogos y manejo de archivos CSV en el Data Lake.
* **Microsoft Fabric:** Ecosistema unificado de datos (Lakehouse + Warehouse).
* **Data Factory:** Orquestación de Pipelines y automatización de ingestas.
* **T-SQL:** Transformaciones complejas, lógica de deduplicación y creación de dimensiones.
* **OneLake:** Almacenamiento "SaaS" basado en el estándar abierto Delta Lake.
* **Power BI:** Visualización de alto impacto utilizando el modo de conexión **Direct Lake**.

---

## 📂 Cómo usar este repositorio
* En la carpeta `/sql` encontrarás los scripts DDL y DML para crear la estructura de las tablas.
* En `/pipelines` se encuentra la lógica de orquestación.
* En `/notebooks` encontrarás el spript de creacion de zonas.
* Revisa la carpeta `/docs` para ver el diagrama del modelo de datos y capturas del dashboard final.
