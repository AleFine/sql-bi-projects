select C.Nombre, C.Ruc, D.Fecha, De.Producto from CLIENTE C inner join DOCUMENTO D on D.Cliente = C.Cliente
inner join DETADOC De on D.Documento = De.Documento inner join PRODUCTO P on P.Producto = De.Producto where C.Cliente = 'CL10'
and MONTH(D.Fecha) IN (1, 6, 12);