# 客户端连接
```shell 
mvn exec:java -D"exec.mainClass"="top.wangbd.mydb.client.Launcher"
```

# 查询所有表
show
# 建表语句
create table student id int64, name string, age int32, (index id name)
# 插入语句
insert into student values 1 "Zhang San" 22
insert into student values 2 "Li Si" 23
insert into student values 3 "Wang Wu" 21
# 查询语句
select * from student
select * from student where id = 1