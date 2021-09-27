# HyperLogLog算法在Presto的应用

## 作业一
> 搜索HyperLogLog算法相关内容，了解其原理，写出5条 HyperLogLog的用途或大数据场景下的实际案例。

HyperLogLog 是一个用于集合去重计数的算法，它是近似相等的，特点：
- 当集合元素数量非常多时，它计算基数所需的空间总是固定的，而且还很小
- 统计规则基于概率完成，统计结果有误差，大约 0.81%

使用：
- Flink 海量数据高效去重统计 UV

## 作业二
> 在本地docker环境或阿里云e-mapreduce环境进行SQL查询， 要求在Presto中使用HyperLogLog计算近似基数。（请自行创 建表并插入若干数据）

https://prestodb.io/docs/current/functions/hyperloglog.html

## 作业三
> 学习使用Presto-Jdbc库连接docker或e-mapreduce环境，重 复上述查询。