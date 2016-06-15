create table concurrent_index_test (
    a int,
    b int
);

insert into concurrent_index_test values(1, 2);
insert into concurrent_index_test values(3, 2);
insert into concurrent_index_test values(4, 2);
insert into concurrent_index_test values(1, 1);
