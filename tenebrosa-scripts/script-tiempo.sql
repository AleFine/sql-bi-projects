--- diseño de tabla (crear dentro del datamart)
CREATE TABLE [dbo].[DimTiempo](
	[KeyTiempo] [int] IDENTITY(1,1) NOT NULL,
	[Anual] [int] NOT NULL,
	[Semestre] [varchar](15) NOT NULL,
	[Trimestre] [varchar](15) NOT NULL,
	[Mes] [varchar](15) NOT NULL,
	[DiaSemana] [varchar](15) NOT NULL,
	[idFecha] [date] NOT NULL,
 CONSTRAINT [PK_DimTiempo] PRIMARY KEY CLUSTERED 
(
	[KeyTiempo] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

--- preparar data: ejecutar desde la bd transaccional
CREATE VIEW v_DatosDimTiempo AS
SELECT DISTINCT 
    YEAR(d.Fecha) AS Año,
    CAST(YEAR(d.Fecha) AS VARCHAR(4)) + '-S' + 
        IIF(DATEPART(QUARTER, d.Fecha) < 3, '1', '2') AS Semestre,
    CAST(YEAR(d.Fecha) AS VARCHAR(4)) + '-T' + 
        CAST(DATEPART(QUARTER, d.Fecha) AS VARCHAR(1)) AS Trimestre,
    DATENAME(MONTH, d.Fecha) AS Mes,
    DATENAME(WEEKDAY, d.Fecha) AS DiaSemana,
    CAST(d.Fecha AS DATE) AS idfecha
FROM DOCUMENTO d

UNION

SELECT DISTINCT 
    YEAR(d.Fepago) AS Año,
    CAST(YEAR(d.Fepago) AS VARCHAR(4)) + '-S' + 
        IIF(DATEPART(QUARTER, d.Fepago) < 3, '1', '2') AS Semestre,
    CAST(YEAR(d.Fepago) AS VARCHAR(4)) + '-T' + 
        CAST(DATEPART(QUARTER, d.Fepago) AS VARCHAR(1)) AS Trimestre,
    DATENAME(MONTH, d.Fepago) AS Mes,
    DATENAME(WEEKDAY, d.Fepago) AS DiaSemana,
    CAST(d.Fepago AS DATE) AS idfecha
FROM CRONOGRAMA d
WHERE d.Fepago IS NOT NULL;
GO

-- Verificación
SELECT V.Año, V.Semestre, V.Trimestre, V.Mes, V.DiaSemana, V.idfecha  
FROM v_DatosDimTiempo V;
GO

-- ETL: DIMENSIÓN TIEMPO
MERGE [TenebrosaMart].[dbo].[DimTiempo] AS dim
USING (
    SELECT Año, Semestre, Trimestre, Mes, DiaSemana, idfecha  
    FROM v_DatosDimTiempo
) AS oltp
ON oltp.idfecha = dim.idfecha
WHEN NOT MATCHED THEN
    INSERT (ANUAL, SEMESTRE, TRIMESTRE, MES, DiaSemana, idfecha)
    VALUES (oltp.Año, oltp.Semestre, oltp.Trimestre, oltp.Mes, oltp.DiaSemana, oltp.idfecha);
GO
