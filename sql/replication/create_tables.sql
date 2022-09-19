create table if not exists customer
(
	id serial not null
		constraint customer_pkey
			primary key,
	name varchar(50),
	address varchar(50) default 'somewhere'::character varying,
	contact varchar(20) default 'user@domain.com'::character varying,
	credit_limit double precision default 44,
	year bigint default (date_part('year'::text, CURRENT_TIMESTAMP))::bigint,
	month bigint default (date_part('month'::text, CURRENT_TIMESTAMP))::bigint,
	day bigint default (date_part('day'::text, CURRENT_TIMESTAMP))::bigint,
	created_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	updated_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint
);

alter table customer owner to postgres;

create trigger set_timestamp
	before update
	on customer
	for each row
	execute procedure trigger_set_timestamp();

create table if not exists manufacturer
(
	id serial not null
		constraint manufacturer_pkey
			primary key,
	name varchar(50),
	address varchar(50) default 'somewhere---'::character varying,
	contact varchar(50) default 'info@company.com'::character varying,
	year bigint default (date_part('year'::text, CURRENT_TIMESTAMP))::bigint,
	month bigint default (date_part('month'::text, CURRENT_TIMESTAMP))::bigint,
	day bigint default (date_part('day'::text, CURRENT_TIMESTAMP))::bigint,
	created_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	updated_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint
);

alter table manufacturer owner to postgres;

create table if not exists "order"
(
	id serial not null
		constraint order_pkey
			primary key,
	customer_id bigint not null
		constraint fk_customer
			references customer,
	reference varchar(50) default 'reference---'::character varying,
	purchase_date bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	year bigint default (date_part('year'::text, CURRENT_TIMESTAMP))::bigint,
	month bigint default (date_part('month'::text, CURRENT_TIMESTAMP))::bigint,
	day bigint default (date_part('day'::text, CURRENT_TIMESTAMP))::bigint,
	created_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	updated_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint
);

alter table "order" owner to postgres;

create table if not exists product
(
	id serial not null
		constraint product_pkey
			primary key,
	manufacture_id bigint not null
		constraint fk_manufacture
			references manufacturer,
	manufacture_date bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	color varchar(50) default 'red'::character varying,
	purchase_date bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	year bigint default (date_part('year'::text, CURRENT_TIMESTAMP))::bigint,
	month bigint default (date_part('month'::text, CURRENT_TIMESTAMP))::bigint,
	day bigint default (date_part('day'::text, CURRENT_TIMESTAMP))::bigint,
	created_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	updated_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint
);

alter table product owner to postgres;

create table if not exists order_items
(
	id serial not null
		constraint order_items_pkey
			primary key,
	order_id bigint not null
		constraint fk_order
			references "order",
	product_id bigint not null
		constraint fk_product
			references product,
	quality integer default 44,
	warranty bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	unit_price real default 444.444,
	year bigint default (date_part('year'::text, CURRENT_TIMESTAMP))::bigint,
	month bigint default (date_part('month'::text, CURRENT_TIMESTAMP))::bigint,
	day bigint default (date_part('day'::text, CURRENT_TIMESTAMP))::bigint,
	created_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint,
	updated_at bigint default (date_part('epoch'::text, CURRENT_TIMESTAMP))::bigint
);

alter table order_items owner to postgres;

