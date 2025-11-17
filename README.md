MYDB: 一个 Java 实现的简单的数据库。实现了以下功能：
- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简易的 SQL 解析
- 基于 socket 的 server 和 client

## 运行方式
注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
```


### Windows下运行

执行以下命令以 D:\temp\mydb 作为路径创建数据库：

```shell
mvn exec:java -D"exec.mainClass"="top.wangbd.mydb.server.Launcher" -D"exec.args"="-create D:\temp\mydb"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -D"exec.mainClass"="top.wangbd.mydb.server.Launcher" -D"exec.args"="-open D:\temp\mydb"
```

这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -D"exec.mainClass"="top.wangbd.mydb.client.Launcher"
```

```markdown
SQL语法:

<begin statement>
    begin [isolation level (read committedrepeatable read)]
        begin isolation level read committed

<commit statement>
    commit

<abort statement>
    abort

<create statement>
    create table <table name>
    <field name> <field type>
    <field name> <field type>
    ...
    <field name> <field type>
    [(index <field name list>)]
        create table students
        id int32,
        name string,
        age int32,
        (index id name)

<drop statement>
    drop table <table name>
        drop table students

<select statement>
    select (*<field name list>) from <table name> [<where statement>]
        select * from student where id = 1
        select name from student where id > 1 and id < 4
        select name, age, id from student where id = 12

<insert statement>
    insert into <table name> values <value list>
        insert into student values 5 "Zhang Yuanjia" 22

<delete statement>
    delete from <table name> <where statement>
        delete from student where name = "Zhang Yuanjia"

<update statement>
    update <table name> set <field name>=<value> [<where statement>]
        update student set name = "ZYJ" where id = 5

<where statement>
    where <field name> (><=) <value> [(andor) <field name> (><=) <value>]
        where age > 10 or age < 3

<field name> <table name>
    [a-zA-Z][a-zA-Z0-9_]*

<field type>
    int32 int64 string

<value>
    .*
```