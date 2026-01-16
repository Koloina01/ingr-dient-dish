Create database mini_dish_db;
\c mini_dish_db;

create user mini_dish_db_user;
alter user mini_dish_db_user with password '0123456';
grant connect on database mini_dish_db to mini_dish_db_user;
grant create on schema public to mini_dish_db_user;
grant select, insert, update, delete on all tables in schema public to mini_dish_db_user;
alter default privileges in schema public grant select, insert, update, delete on tables to mini_dish_db_user;