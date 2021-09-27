## 作业一

### 题目

> 1.思考题：如何避免小文件问题 如何避免小文件问题？给出2～3种解决方案
> 

#### 小文件会导致的问题
1. HDFS上每个文件都要在namenode上建立一个索引，这个索引的大小约为150byte，这样当小文件比较多的时候，就会产生很多的索引文件，一方面会大量占用namenode的内存空间，另一方面就是索引文件过大是的索引速度变慢。
2. 小文件会导致创建过多的计算任务


#### 方案1

在任务的执行过程中： repartition() OR coalesce()

#### 方案2

使用 shell 命令定期合并小文件

#### 方案3

使用 Ozone

## 作业二

### 题目

> 实现Compact table command

添加compact table命令，用于合并小文件，例如表test1总共有50000个文件， 每个1MB，通过该命令，合成为500个文件，每个约100MB。

**语法：**

``` sparksql
COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES];
```

**说明：**

1. 如果添加partitionSpec，则只合并指定的partition目录的文件。
2. 如果不加into fileNum files，则把表中的文件合并成128MB大小。
3. 以上两个算附加要求，基本要求只需要完成以下功能： COMPACT TABLE test1 INTO 500 FILES;

**参考代码：**
SqlBase.g4:

```antlrv4
| COMPACT TABLE target=tableIdentifier partitionSpec?
(INTO fileNum=INTEGER_VALUE identifier)? #compactTable
```

## 作业三

### 题目

> Insert命令自动合并小文件

- 我们讲过AQE可以自动调整reducer的个数，但是正常跑Insert命 令不会自动合并小文件，例如insert into t1 select * from t2;
- 请加一条物理规则（Strategy），让Insert命令自动进行小文件合 并(repartition)。（不用考虑bucket表，不用考虑Hive表）

**参考代码：**

```scala
object RepartitionForInsertion extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan transformDown { case i@InsertIntoDataSourceExec(child, _, _, partitionColumns, _)...
      val newChild =
      ...
      i.withNewChildren(newChild :: Nil)
    }
  }
}
```