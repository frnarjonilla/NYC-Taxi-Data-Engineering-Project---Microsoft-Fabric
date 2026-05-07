CREATE TABLE [dbo].[Silver_Viajes_Taxis] (

	[ID_Vendedor] int NULL, 
	[Fecha_Recogida] datetime2(6) NULL, 
	[Fecha_Entrega] datetime2(6) NULL, 
	[Numero_Pasajeros] int NULL, 
	[Distancia_Viaje] float NULL, 
	[ID_Lugar_Recogida] int NULL, 
	[ID_Lugar_Entrega] int NULL, 
	[Tarifa_Base] decimal(10,2) NULL, 
	[Propina] decimal(10,2) NULL, 
	[Monto_Total] decimal(10,2) NULL, 
	[ID_Tipo_Pago] int NULL
);