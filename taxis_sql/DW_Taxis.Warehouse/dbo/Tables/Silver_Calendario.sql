CREATE TABLE [dbo].[Silver_Calendario] (

	[ID_Fecha] date NULL, 
	[Anio] int NULL, 
	[Mes_Numero] int NULL, 
	[Mes_Nombre] varchar(20) NULL, 
	[Semana_Anio] int NULL, 
	[Dia_Semana] varchar(20) NULL, 
	[Tipo_Dia] varchar(13) NOT NULL
);