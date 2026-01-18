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