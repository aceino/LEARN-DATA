create table if not exists customers ( 
    customer_id serial primary key, 
    first_name text,
    last_name text, 
    email text, 
    address text , 
    created_at timestamp default now(),
    updated_at timestamp default now()
);

alter table customers replica identity full; 

create table if not exists orders ( 
    order_id serial primary key, 
    customer_id int references customers(customer_id), 
    product_id int references orders(product_id), 
    total_amount numeric(10,2) default 0,
    status text, 
    created_at timestamp default now(),
    updated_at timestamp default now()
);

lter table orders replica identity full; 

create table if not exists products ( 
    product_id serial primary key , 
    sku text unique not null, 
    name text not null, 
    description text ,
    unit_price numeric(10, 2) not null,
    created_at timestamp default now() ,
    updated_at timestamp default now()
);

alter table products replica identity full; 

create table if not exists order_items( 
    item_id serial primary key, 
    order_id int not null references orders(order_id) on delete cascade, 
    product_id int not null references products(product_id), 
    quantity int not null, 
    unit_price numeric(10, 2) not null, 
    line_total numeric(12, 2) generated always as (quantity * unit_price) stored, 
    created_at timestamp default now(),
    updated_at timestamp default now()
);

alter table order_items replica identity full; 

create table if not exists cdc_audit( 
    id serial primary key,  
    event_time timestamp default now() ,
    source_table varchar(50), 
    operation char(1),
    pk_value varchar(100), 
    before_json JSONB,
    after_json JSONB
);

create publication cdc_publication for table customers, orders, products, order_items;

