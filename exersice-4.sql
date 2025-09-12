use TenebrosaOLTP;

select *from CLIENTE 

select  C.Cliente as Cliente,  C.Cliente as Nombre, C.Saldo, C.Calificacion, C.credito as Cliente 
from CLIENTE C inner join DOCUMENTO D on D.Cliente = C.Cliente 
where C.credito = 1 and C.Saldo > 0;