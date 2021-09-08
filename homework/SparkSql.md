## 作业一



![运行结果](../resource/spark03.png)


## 作业二
### 题目 
> 构建SQL满足如下要求,通过 `set spark.sql.planChangeLog.level=WARN;` 查看 

#### 构建一条SQL，同时apply下面三条优化规则： 
- CombineFilters 
- CollapseProject 
- BooleanSimplification

#### 构建一条SQL，同时apply下面五条优化规则：
- ConstantFolding 
- PushDownPredicates 
- ReplaceDistinctWithAggregate 
- ReplaceExceptWithAntiJoin 
- FoldablePropagation

### 代码一

### 运行结果一


### 代码二
```sql
select *
from (
         select customerId, amountPaid, 1.0
         FROM (select customerId, amountPaid from sales) a EXCEPT (select customerId, amountPaid, 1.0 x
                                                                   from sales
                                                                   where customerId = 200 + 300)
     )
where amountPaid = 600;
```
### 运行结果二
> ConstantFolding 常数折叠
![运行结果](../resource/ConstantFolding.png)
 
> PushDownPredicates 谓词下推
![运行结果](../resource/PushDownPredicates.png)

> ReplaceDistinctWithAggregate
![运行结果](../resource/ReplaceDistinctWithAggregate.png)

> ReplaceExceptWithAntiJoin
![运行结果](../resource/ReplaceExceptWithAntiJoin.png)

> FoldablePropagation 可折叠算子简化
![运行结果](../resource/FoldablePropagation.png)