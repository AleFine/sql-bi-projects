MERGE [TenebrosaMart].DBO.DimEficiencia AS dim USING
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

MERGE [TenebrosaMart].DBO.DimOrganizacion AS dim USING
(SELECT Tienda.Descripcion AS Sucursal, PERSONAL.Nombre AS Sectorista, 
PERSONAL.Personal AS idPersonal
FROM  Tienda INNER JOIN  PERSONAL ON Tienda.idTienda = PERSONAL.idTienda) AS oltp
ON oltp.idpersonal = dim.idPersonal
WHEN NOT MATCHED THEN
	INSERT (Sucursal, Sectorista, idPersonal)
	VALUES (Sucursal, Sectorista, idPersonal);


go

MERGE TenebrosaMart.dbo.DimProducto AS dim USING
(SELECT     LINEA.Descripcion AS Linea, MARCA.Descripcion AS Marca, PRODUCTO.Producto AS idProducto, PRODUCTO.Descripcion AS Producto, 
                      PROVEEDOR.RazonSocial
FROM         LINEA INNER JOIN
                      MARCA ON LINEA.Linea = MARCA.Linea INNER JOIN
                      PRODUCTO ON MARCA.Marca = PRODUCTO.Marca INNER JOIN
                      PROVEEDOR ON MARCA.Proveedor = PROVEEDOR.Proveedor) as oltp
ON oltp.idProducto = dim.idProducto 
WHEN NOT MATCHED THEN
	INSERT (Linea, Proveedor, Marca, Producto, idProducto)
	VALUES (Linea, RazonSocial, Marca, Producto, idProducto) ;
go
MERGE [MartComercial].DBO.DimSatisfaccion AS dim USING
(SELECT DISTINCT D.Satisfaccion,
NPS = CASE WHEN D.Satisfaccion<7 THEN 'DETRACTORES'
			WHEN D.Satisfaccion<9 THEN 'REGULARES' ELSE 'PROMOTORES' END
FROM DOCUMENTO D) AS oltp
ON DIM.idValor =oltp.Satisfaccion
WHEN NOT MATCHED THEN
	INSERT (NPS, Valor, idValor)
	VALUES (NPS, Satisfaccion, Satisfaccion);
go
--- preparar data: ejecutar desde la bd transaccional
CREATE VIEW v_DatosDimTiempo AS
SELECT DISTINCT Año = YEAR(d.fecha), 
SEMESTRE = DATENAME(YY, d.Fecha) + '-S' +  IIF(DATENAME(qq, d.Fecha) <3 ,'1', '2'),
TRIMESTRE= DATENAME(YY, d.Fecha) + '-T' +  DATENAME(qq, d.Fecha),
MES= DATENAME(MM, d.Fecha),  DiaSemana = DATENAME(dw, d.Fecha),
idfecha = CAST(d.Fecha as date )
FROM DOCUMENTO d
UNION
SELECT DISTINCT Año = YEAR(d.Fepago), 
SEMESTRE = DATENAME(YY, d.Fepago) + '-S' +  IIF(DATENAME(qq, d.Fepago) <3 ,'1', '2'),
TRIMESTRE= DATENAME(YY, d.Fepago) + '-T' +  DATENAME(qq, d.Fepago),
MES= DATENAME(MM, d.Fepago),  DiaSemana = DATENAME(dw, d.Fepago),
idfecha = CAST(d.Fepago as date )
FROM cronograma d
where Fepago is not null
go
SELECT V.Año, V.SEMESTRE, V.TRIMESTRE, V.MES, V.DiaSemana, V.idfecha  FROM v_DatosDimTiempo V
-- ETL: DIMENSION TIEMPO
MERGE [TenebrosaMart].[dbo].[DimTiempo] AS dim USING 
(SELECT V.Año, V.SEMESTRE, V.TRIMESTRE, V.MES, V.DiaSemana, V.idfecha  FROM v_DatosDimTiempo V) AS oltp
ON oltp.idfecha = dim.idfecha
WHEN NOT MATCHED THEN
	INSERT (ANUAL, SEMESTRE, TRIMESTRE, MES, DiaSemana, idfecha)
	VALUES (Año, SEMESTRE, TRIMESTRE, MES, DiaSemana, idfecha);
go
MERGE TenebrosaMart.dbo.DimProducto AS dim USING
(SELECT     LINEA.Descripcion AS Linea, MARCA.Descripcion AS Marca, PRODUCTO.Producto AS idProducto, PRODUCTO.Descripcion AS Producto, 
                      PROVEEDOR.RazonSocial
FROM         LINEA INNER JOIN
                      MARCA ON LINEA.Linea = MARCA.Linea INNER JOIN
                      PRODUCTO ON MARCA.Marca = PRODUCTO.Marca INNER JOIN
                      PROVEEDOR ON MARCA.Proveedor = PROVEEDOR.Proveedor) as oltp
ON oltp.idProducto = dim.idProducto 
WHEN NOT MATCHED THEN
	INSERT (Linea, Proveedor, Marca, Producto, idProducto)
	VALUES (Linea, RazonSocial, Marca, Producto, idProducto) ;
go