create table Transaction(
	Tid varchar(20),
	TtimeStamp int,
	Tstatus varchar(20),
    Items varchar(200),
    Operation varchar(500)
);


create table lockTable(
	item varchar(20),
	state varchar(50),
	Tid_holding varchar(200),
    Tid_waiting varchar(200)
);
