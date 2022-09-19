insert into customer(name) select md5(random()::text) from generate_series(0,100000);
insert into manufacturer(name) select md5(random()::text) from generate_series(0,100000);
insert into product(manufacture_id)  select random()*100+1 from generate_series(0,100000);
insert into "order"(customer_id) select random()*100+1 from generate_series(0,100000);
insert into order_items(order_id, product_id)  select random()*100+1, random()*100+1 from generate_series(0,100000/2);
