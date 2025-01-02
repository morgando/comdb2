begin
create table t { schema { int a } keys { "a" = a } constraints { "a" -> "q":"a" on delete cascade } }$$
create table q { schema { int a } keys { "a" = a } }$$
commit
insert into t values(1)
select 'shouldnt see me', * from t
insert into q values(1)
insert into t values(1)
select 'should see me', * from t
select 'should see me', * from q
delete from q where 1
select 'shouldnt see me', * from t
select 'shouldnt see me', * from q
