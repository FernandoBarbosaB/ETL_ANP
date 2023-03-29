CREATE DATABASE Raizen;

CREATE SCHEMA Stage;
CREATE SCHEMA Final;



CREATE TABLE Stage.derivados_petroleo(
	COMBUSTÍVEL VARCHAR(30),
	ANO INT, 
	REGIÃO VARCHAR(50),
	ESTADO VARCHAR(50),
	UNIDADE VARCHAR(3),
	Jan FLOAT,
	Fev FLOAT,
    Mar FLOAT,
	Abr FLOAT,
	Mai FLOAT,
	Jun FLOAT,
	Jul FLOAT,
	Ago FLOAT,
	"Set" FLOAT,
	"Out" FLOAT,
	Nov FLOAT,
	Dez FLOAT,
	TOTAL FLOAT
)


CREATE TABLE Final.derivados_petroleo(
	year_month DATE,
	uf VARCHAR(50),
	product VARCHAR(30),
	unit VARCHAR(2),
	volume FLOAT,
	created_at DATETIME
)


SELECT * FROM Stage.derivados_petroleo

SELECT * FROM Final.derivados_petroleo



CREATE TABLE Stage.diesel(
	COMBUSTÍVEL VARCHAR(30),
	ANO INT, 
	REGIÃO VARCHAR(50),
	ESTADO VARCHAR(50),
	UNIDADE VARCHAR(3),
	Jan FLOAT,
	Fev FLOAT,
    Mar FLOAT,
	Abr FLOAT,
	Mai FLOAT,
	Jun FLOAT,
	Jul FLOAT,
	Ago FLOAT,
	"Set" FLOAT,
	"Out" FLOAT,
	Nov FLOAT,
	Dez FLOAT,
	TOTAL FLOAT
)


CREATE TABLE Final.diesel(
	year_month DATE,
	uf VARCHAR(50),
	product VARCHAR(30),
	unit VARCHAR(2),
	volume FLOAT,
	created_at DATETIME
)


SELECT * FROM Stage.diesel


SELECT * FROM Final.diesel;


