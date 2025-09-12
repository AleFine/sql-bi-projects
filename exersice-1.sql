USE TenebrosaOLTP;

SELECT 
	DT.Cantidad,
	DT.Documento,
	DT.Producto,
	DT.Cantidad,
	DT.PrecUnit,
	(Cantidad * PrecUnit) AS MONTOS
FROM DETADOC DT
WHERE (Cantidad * PrecUnit) < 100000 AND (Cantidad * PrecUnit)>20000 AND
Producto='PR01';

