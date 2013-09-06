select load_extension('./libredis_vtbl.so');

create virtual table basic_test using redis (localhost, basic, 
    c0 varchar,
    c1 int,
    c2 float
);

insert into basic_test (c0, c1, c2) values ('hello', 1, 3.14159);

select * from basic_test;
select c0 from basic_test where c2 > 3;
select c1 from basic_test where c1 > 0 and c0 = 'hello' and c2 > 3.1;

delete from basic_test;
drop table basic_test;

