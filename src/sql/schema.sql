create type dishtypes as enum (
    'START',
    'MAIN',
    'DESSERT'
);

create table Dish (
    id int primary key,
    name varchar(250) not null,
    dish_type dishtypes not null
);

create type ingredient_types as enum (
    'VEGETABLE',
    'ANIMAL',
    'MARINE',
    'DAIRY',
    'OTHER'
);

create table Ingredient (
    id int primary key,
    name varchar(250) not null,
    price numeric(10,2) not null,
    category ingredient_types not null,
    id_dish int references Dish(id)
);