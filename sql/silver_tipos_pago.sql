/* =============================================================================
CAPA SILVER: DIMENSIÓN DE TIPOS DE PAGO
=============================================================================
Descripción: 
Creación manual de la tabla maestra de Tipos de Pago. Los datos de origen 
solo contienen IDs numéricos; esta tabla permite la interpretación de 
dichos códigos en el modelo semántico.

Hitos técnicos:
1. Creación de estructura DDL con tipos de datos optimizados (INT, VARCHAR).
2. Mapeo manual de códigos oficiales de la TLC (Taxi & Limousine Commission).
3. Estandarización de categorías para reporting en Power BI.
=============================================================================
*/

-- 1. Creación de la estructura de la tabla física
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Silver_Tipos_Pago')
BEGIN
    CREATE TABLE dbo.Silver_Tipos_Pago (
        ID_Tipo_Pago INT,
        Descripcion_Pago VARCHAR(50)
    );
END

-- 2. Carga de datos maestros (Referencia oficial de NYC Taxi)
-- Limpiamos antes de insertar para evitar duplicados en caso de re-ejecución
TRUNCATE TABLE dbo.Silver_Tipos_Pago;

INSERT INTO dbo.Silver_Tipos_Pago (ID_Tipo_Pago, Descripcion_Pago)
VALUES 
(1, 'Tarjeta de Crédito'),
(2, 'Efectivo'),
(3, 'Sin Cargo'),
(4, 'Disputa'),
(5, 'Desconocido'),
(6, 'Viaje Anulado');

-- 3. Verificación de la carga
SELECT * FROM dbo.Silver_Tipos_Pago;
