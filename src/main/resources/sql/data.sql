insert into dish (id, name, dish_type)
values (1, 'Salaide fraîche', 'STARTER'),
       (2, 'Poulet grillé', 'MAIN'),
       (3, 'Riz aux légumes', 'MAIN'),
       (4, 'Gâteau au chocolat ', 'DESSERT'),
       (5, 'Salade de fruits', 'DESSERT');


insert into ingredient (id, name, category, price, id_dish)
values (1, 'Laitue', 'VEGETABLE', 800.0, 1),
       (2, 'Tomate', 'VEGETABLE', 600.0, 1),
       (3, 'Poulet', 'ANIMAL', 4500.0, 2),
       (4, 'Chocolat ', 'OTHER', 3000.0, 4),
       (5, 'Beurre', 'DAIRY', 2500.0, 4);



update dish
set price = 2000.0
where id = 1;

update dish
set price = 6000.0
where id = 2;

insert into DishIngredient (id_dish, id_ingredient, quantity_required, unit)
values (1, 1, 0.20, 'KG'),
        (1, 2, 0.15, 'KG'),
        (2, 3, 1.00, 'KG'),
        (4, 4, 0.30, 'KG'),
        (4, 5, 0.20, 'KG');

update dish set selling_price = 3500.00 where id = 1;
update dish set selling_price = 12000.00 where id = 2;
update dish set selling_price = null where id = 3;
update dish set selling_price = 8000.00 where id = 4;
update dish set selling_price = null where id = 5;

insert into StockMovement (id_ingredient, quantity, type, unit, creation_datetime)
values
(1, 5.0, 'IN', 'KG', '2024-01-05 08:00'),
(1, 0.2, 'OUT', 'KG', '2024-01-06 12:00'),
(2, 4.0, 'IN', 'KG', '2024-01-05 08:00'),
(2, 0.15, 'OUT', 'KG', '2024-01-06 12:00'),
(3, 10.0, 'IN', 'KG', '2024-01-04 09:00'),
(3, 1.0, 'OUT', 'KG', '2024-01-06 13:00'),
(4, 3.0, 'IN', 'KG', '2024-01-05 10:00'),
(4, 0.3, 'OUT', 'KG', '2024-01-06 14:00'),
(5, 2.5, 'IN', 'KG', '2024-01-05 10:00'),
(5, 0.2, 'OUT', 'KG', '2024-01-06 14:00');
