/* =============================================================================
CAPA SILVER: DIMENSIÓN DE UBICACIONES (ZONAS)
=============================================================================
Descripción: 
Este script transforma la tabla 'bronze_zonas' en una tabla física 
estructurada dentro del Data Warehouse.

Hitos técnicos:
1. Renombrado de columnas a lenguaje de negocio (Distrito, Barrio).
2. Tipado de datos (Casting a INT) para optimizar JOINs.
3. Filtrado de valores nulos para asegurar la integridad referencial.
=============================================================================
*/

-- 1. Eliminamos la tabla si ya existe para asegurar una carga limpia (Opcional)
DROP TABLE IF EXISTS dbo.Silver_Zonas;

-- 2. Creamos la tabla física con nombres claros y tipos de datos definidos
CREATE TABLE dbo.Silver_Zonas
AS
SELECT 
    CAST(LocationID AS INT) AS ID_Ubicacion,
    Borough AS Distrito,
    Zone AS Barrio,
    service_zone AS Zona_Servicio
FROM [Lakehouse_Taxis_NY].[inc].[bronze_zonas]
WHERE LocationID IS NOT NULL;
