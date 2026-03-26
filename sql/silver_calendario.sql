/* =============================================================================
CAPA SILVER: DIMENSIÓN DE CALENDARIO (TIEMPO)
=============================================================================
Descripción: 
Generación dinámica de una tabla de tiempos para los años 2019 y 2020. 
Esta tabla es el eje central para realizar análisis de Inteligencia de Tiempo 
(Time Intelligence) en Power BI.

Hitos técnicos:
1. Generación de series mediante CTEs multiplicadoras (eficiencia máxima).
2. Cálculo de atributos temporales (Mes, Año, Semana, Día de la semana).
3. Lógica de negocio para clasificar días (Fin de Semana vs. Laborable).
=============================================================================
*/

-- 1. Eliminamos si existe para asegurar una carga limpia
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'Silver_Calendario')
    DROP TABLE dbo.Silver_Calendario;

-- 2. Generación dinámica de fechas mediante producto cartesiano de CTEs
CREATE TABLE dbo.Silver_Calendario AS
WITH 
    E1(N) AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1), -- 10
    E2(N) AS (SELECT 1 FROM E1 a, E1 b), -- 100
    E4(N) AS (SELECT 1 FROM E2 a, E2 b), -- 10,000 
    cteTally(N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E4),
    ListaFechas AS (
        SELECT DATEADD(DAY, N - 1, '2019-01-01') AS Fecha
        FROM cteTally
        -- Filtramos para cubrir exactamente el rango de nuestro análisis
        WHERE N <= DATEDIFF(DAY, '2019-01-01', '2020-12-31') + 1 
    )
SELECT 
    CAST(Fecha AS DATE) AS ID_Fecha,
    YEAR(Fecha) AS Anio,
    MONTH(Fecha) AS Mes_Numero,
    -- Mes_Nombre y Dia_Semana para facilitar filtros visuales
    CAST(DATENAME(MONTH, Fecha) AS VARCHAR(20)) AS Mes_Nombre,
    DATEPART(WEEK, Fecha) AS Semana_Anio,
    CAST(DATENAME(WEEKDAY, Fecha) AS VARCHAR(20)) AS Dia_Semana,
    -- Clasificación útil para análisis de demanda de transporte
    CASE 
        WHEN DATEPART(WEEKDAY, Fecha) IN (1, 7) THEN 'Fin de Semana' 
        ELSE 'Laborable' 
    END AS Tipo_Dia
FROM ListaFechas;

-- 3. Verificación de la integridad del rango generado
SELECT MIN(ID_Fecha) as Inicio, MAX(ID_Fecha) as Fin, COUNT(*) as Total_Dias 
FROM dbo.Silver_Calendario;
