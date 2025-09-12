use TenebrosaOLTP;


select D.Documento as Documento,  
	D.TipoDoc as 'Tipo de Documento',
	De.Producto as 'Codigo de Producto',
	P.Descripcion as Descripcion,
	De.Cantidad as Cantidad,
	De.PrecUnit * Cantidad as 'Subtotal',
	C.Nombre as 'Cliente',
	D.Fecha as Fecha
from DOCUMENTO D inner join DETADOC De on D.Documento = De.Documento 
inner join PRODUCTO P on De.Producto = P.Producto inner join Cliente C
on C.Cliente = D.Cliente where Year(D.Fecha) = '2009'