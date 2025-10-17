----21.	Se pretende conocer en qué días de la semana, 
---se han realizado las mayores ventas del primer trimestre y Año 2006 determinado de la marca M1. 

-- condiciones: primer trimestre y Año 2006 determinado de la marca M1.
-- mostrar: dias semana

SELECT DATENAME(dw, d.Fecha) as diaSemana, SUM(dd.cantidad* dd.precunit) As Venta
FROM DOCUMENTO d INNER JOIN DETADOC dd ON d.Documento = dd.Documento and d.TipoDoc = dd.TipoDoc
	INNER JOIN PRODUCTO P on P.Producto = dd.Producto
WHERE YEAR(D.FECHA) = 2006 AND DATEPART(QQ, d.Fecha) = 1 AND P.Marca='M1'
group by DATENAME(dw, d.Fecha)
HAVING SUM(dd.cantidad* dd.precunit)  >491050.1368
order by 2 DESC

----  los que superado : 491050.1368

SELECT TOP 3 DATENAME(dw, d.Fecha) as diaSemana, SUM(dd.cantidad* dd.precunit) As Venta
FROM DOCUMENTO d INNER JOIN DETADOC dd ON d.Documento = dd.Documento and d.TipoDoc = dd.TipoDoc
	INNER JOIN PRODUCTO P on P.Producto = dd.Producto
WHERE YEAR(D.FECHA) = 2006 AND DATEPART(QQ, d.Fecha) = 1 AND P.Marca='M1'
group by DATENAME(dw, d.Fecha)
-- HAVING SUM(dd.cantidad* dd.precunit)  >491050.1368
order by 2 DESC

----  los que 3 DIAS QUE MAS SE VENDIO