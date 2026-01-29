alter table ingredient drop column if exists id_dish;
alter table ingredient drop column if exists required_quantity;
create type unit_type as enum ('PCS', 'KG', 'L');
alter table dish add column selling_price numeric(10,2);


create table DishIngredient (
    id serial primary key,
    id_dish int references dish (id),
    id_ingredient int references ingredient (id),
    quantity_required numeric(10, 2),
    unit unit_type
);

alter table Dish drop column selling_price;

alter table Ingredient
    add column if not exists required_quantity numeric(10, 2);
create type mouvement_type as enum ('IN', 'OUT');
create table StockMovement (
    id serial primary key,
    id_ingredient int references ingredient (id),
    quantity numeric(10, 2),
    type mouvement_type,
    unit unit_type,
    creation_datetime timestamp default current_timestamp
);

create table if not exists "order"
(
    id                serial primary key,
    reference         varchar(255),
    creation_datetime timestamp without time zone
);

create table if not exists dish_order
(
    id       serial primary key,
    id_order int references "order" (id),
    id_dish  int references dish (id),
    quantity int
);

CREATE TYPE order_type_enum AS ENUM ('EAT_IN', 'TAKE_AWAY');
CREATE TYPE order_status_enum AS ENUM ('CREATED', 'READY', 'DELIVERED');

ALTER TABLE "order"
ADD COLUMN type VARCHAR(20) DEFAULT 'EAT_IN',
ADD COLUMN status VARCHAR(20) DEFAULT 'CREATED';

ALTER TABLE "order"
DROP COLUMN type,
DROP COLUMN status;

ALTER TABLE "order"
ADD COLUMN type order_type_enum DEFAULT 'EAT_IN',
ADD COLUMN status order_status_enum DEFAULT 'CREATED';
