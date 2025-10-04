SELECT 
	c1.nombre as cliente, 
	z.Descripcion as Zona, 
	c1.Cliente as idcliente,

	Calificacion = CASE WHEN c1.calificacion = 'a' THEN 'OPTIMO'
						   WHEN c1.calificacion = 'b' THEN 'BUENA' 
						   ELSE 'NORMAL' 
						   end,
	c1.TipoCliente
FROM cliente c1 
inner join ZONA z on z.Zona = c1.Zona
inner join MULTITABLA mt on mt.Valor = c1.TipoCliente AND mt.Tipo='01'

select * from MULTITABLA

MERGE [TenebrosaMart2].