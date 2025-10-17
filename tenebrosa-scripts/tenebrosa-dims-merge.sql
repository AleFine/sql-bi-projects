--- dim eficiencia

MERGE [Mart_Tenebrosa].DBO.DimEficiencia AS dim USING
(select DISTINCT plazo= DATEDIFF(dd, FechaProgramada, fechaEntrega),
TipoPlazo = CASE WHEN DATEDIFF(dd, FechaProgramada, fechaEntrega)<=0 THEN 'BUENO'
WHEN DATEDIFF(dd, FechaProgramada, fechaEntrega)<=2 THEN 'REGULAR'
WHEN DATEDIFF(dd, FechaProgramada, fechaEntrega)<=5 THEN 'MALO' ELSE  'MUY MALO'
	END
from documento) AS oltp
ON oltp.Plazo = dim.idPlazo
WHEN NOT MATCHED THEN
	INSERT (TipoPlazo, Plazo, idPlazo)
	VALUES (TipoPlazo, Plazo, Plazo);
go
select * from [Mart_Tenebrosa].DBO.DimEficiencia 

--------------

MERGE [Mart_Tenebrosa].DBO.DimCliente AS dim USING
(SELECT c.Nombre, TipoCliente = mt.Descripcion , 
Calificacion = case when c.Calificacion='a' then 'ALTA' when c.Calificacion ='b' then 'MEDIA' else 'NORMAL' end, 
c.Cliente as idCliente, z.Descripcion AS zona, cd.Nombre as Ciudad
FROM cliente c INNER JOIN ZONA z ON C.Zona = Z.Zona 
		INNER JOIN CIUDAD cd ON cd.idCiudad = z.idCiudad
		inner join MULTITABLA mt ON mt.Valor = c.TipoCliente and mt.tipo ='01') AS oltp
ON oltp.idcliente = dim.idcliente
WHEN NOT MATCHED THEN
	INSERT (Cliente, TipoCliente, Categoria, Zona, Ciudad, idCliente)
	VALUES (Nombre, TipoCliente, Calificacion, Zona, Ciudad, idCliente);
go
select * from [Mart_Tenebrosa].DBO.DimCliente 
-----------------tabla dim Eficiencia

CREATE TABLE [dbo].[DimEficiencia](
	[KeyEficiencia] [int] IDENTITY(1,1) NOT NULL,
	[TipoPlazo] [varchar](50) NOT NULL,
	[Plazo] [int] NOT NULL,
	[idPlazo] [int] NOT NULL,
 CONSTRAINT [PK_DimEficiencia] PRIMARY KEY CLUSTERED 
(
	[KeyEficiencia] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO