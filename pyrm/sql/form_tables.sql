create schema form;

create table form.product_master (
	id serial primary key,
	pn varchar(12) not null unique,
	pn_desc varchar(50) null
);

create table form.lot_master (
	id serial primary key,
	lot_no varchar(12) not null unique,
	mfg_date varchar(20) null,
	exp_date varchar(20) null,
	mfg_by varchar(20) null,
	mfg_site varchar(10) NULL,
	ipt_by varchar(20) null,
	ipt_date varchar(20) null,
	pn_id int references form.product_master(id) on delete cascade
);

create table form.lineage (
	id serial primary key,
	from_desc varchar(100) null,
	from_pn varchar(12) not null,
	from_ln varchar(100) null,
	to_pn_id int references form.product_master(id) on delete cascade,
	to_ln_id int references form.lot_master(id) on delete cascade
);

create table form.ipt (
	id serial primary key,
	data_name varchar(100) not null,
	data_value varchar(50) null,
	uom varchar(25) null,
	ln_id int references form.lot_master(id) on delete cascade,
	pn_id int references form.product_master(id) on delete cascade
);