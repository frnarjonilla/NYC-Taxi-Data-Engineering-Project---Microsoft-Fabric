/* =============================================================================
ESTRATEGIA: CARGA INCREMENTAL UNIVERSAL Y AUTOMATIZADA
=============================================================================
Descripción: 
Este script realiza la ingesta desde la capa Bronze (Lakehouse) a la 
capa Silver (Warehouse). 

Hitos técnicos:
1. Creación de tabla con tipos de datos optimizados (Decimal, Datetime2).
2. Extracción dinámica del mes mediante el nombre del archivo del parámetro.
3. Lógica de deduplicación mediante clave compuesta (NOT EXISTS).
=============================================================================
*/

-- 1. Verificación y Creación de la Tabla Silver
IF NOT EXISTS (
    SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE t.name = 'Silver_Viajes_Taxis' AND s.name = 'dbo'
)
BEGIN
    CREATE TABLE dbo.Silver_Viajes_Taxis (
        ID_Vendedor INT,
        Fecha_Recogida DATETIME2(6),
        Fecha_Entrega DATETIME2(6),
        Numero_Pasajeros INT,
        Distancia_Viaje FLOAT,
        ID_Lugar_Recogida INT,
        ID_Lugar_Entrega INT,
        Tarifa_Base DECIMAL(10,2),
        Propina DECIMAL(10,2),
        Monto_Total DECIMAL(10,2),
        ID_Tipo_Pago INT
    );
END

-- 2. Inserción Inteligente (Capa Bronze -> Silver)
INSERT INTO dbo.Silver_Viajes_Taxis
SELECT 
    CAST(VendorID AS INT),
    CAST(tpep_pickup_datetime AS DATETIME2(6)),
    CAST(tpep_dropoff_datetime AS DATETIME2(6)),
    CAST(passenger_count AS INT),
    CAST(trip_distance AS FLOAT),
    CAST(PULocationID AS INT),
    CAST(DOLocationID AS INT),
    CAST(fare_amount AS DECIMAL(10,2)),
    CAST(tip_amount AS DECIMAL(10,2)),
    CAST(total_amount AS DECIMAL(10,2)),
    CAST(payment_type AS INT)
FROM [Lakehouse_Taxis_NY].[inc].[Bronze_Viajes_Taxis] AS origen
WHERE 
    -- Filtro de Seguridad: Solo registros del año 2019
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2(6))) = 2019
    
    -- LÓGICA DINÁMICA: 
    -- Buscamos '2019-' en el nombre del archivo y saltamos 5 espacios para obtener el mes.
    -- Esto permite procesar cualquier mes sin modificar el código.
    AND MONTH(CAST(tpep_pickup_datetime AS DATETIME2(6))) = 
        CAST(SUBSTRING('@{pipeline().parameters.Mes_Carga}', 
             CHARINDEX('2019-', '@{pipeline().parameters.Mes_Carga}') + 5, 2) AS INT)

    AND VendorID IS NOT NULL
    
    -- GESTIÓN DE DUPLICADOS:
    -- Verificamos que el viaje no exista previamente mediante clave compuesta.
    AND NOT EXISTS (
        SELECT 1 FROM dbo.Silver_Viajes_Taxis AS destino
        WHERE destino.Fecha_Recogida = CAST(origen.tpep_pickup_datetime AS DATETIME2(6))
        AND destino.ID_Vendedor = CAST(origen.VendorID AS INT)
        AND destino.Monto_Total = CAST(origen.total_amount AS DECIMAL(10,2))
    );
