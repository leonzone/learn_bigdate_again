# Hive Sql 练习
## 素材准备
### t_movie
```sql
create table t_movie
(
	movieid bigint,
	moviename string,
	movietype string
);


```
[t_movie 数据准备](../resource/t_movie.csv)
### t_rating
```sql
create  table t_rating
(
	userid bigint,
	movieid bigint,
	rate double,
	times string
);


```
[t_rating 数据准备](../resource/t_rating.csv)
### t_user
```sql
CREATE TABLE t_user
(
    userid     bigint,
    sex        string,
    age        int,
    occupation string,
    zipcode    string
);
```
[t_user 数据准备](../resource/t_user.csv)

## 题目一：

> 展示电影ID为2116这部电影各年龄段的平均影评分


### 我先思考
- 首先确定主表，既然是评分故选择 `t_rating` 
- `t_rating` 有电影ID和评分的信息，但是没有年龄信息
- 年龄信息通过 `t_user` 获得，并通过 `userid` 进行关联
- 通过使用 `GROUP BY` 将年龄分段，`AVG()` 获得平均平凡
- `ORDER BY` 让年龄从小到大展示

### Show Me Code
```sql
SELECT tu.age, AVG(tr.rate)
FROM t_rating tr
         LEFT JOIN t_user tu ON tr.userid = tu.userid
WHERE tr.movieid = 2116
GROUP BY tu.age
ORDER BY tu.age;
```

### 运行结果
![](../resource/hive01.png)

## 题目二：

> 找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数

### 我先思考
- 通过 `t_rating left join userid` 将评分信息和用户信息进行关联
- 我们需要筛选出男性，可以先将 `t_user` 过滤出男性后再 `join`, 只有 `join` 上的表才为男性评分
- 获取评分最高的电影需要使用 `GROUP BY` 对电影进行分组，并求得评分的平均值（因为每部电影的评分数不一样所以不能用总评分）  
- 还要筛选评分次数超过50次的，这里需要使用 `HAVING total > 50`, `totol` 是在 `SELECT` 语句中通过函数 `COUNT(1) AS total` 获得的，这也展示了 `WHERE` 和 `HAVING` 的区别
  - `HAVING` 必须和 `GROUP BY` 配合使用
  - `WHERE` 只能过滤已有字段，`HAVING` 不仅能过滤已有字段，还能过滤计算后的字段
    - 对于已有字段，推荐先使用 `WHERE` 过滤，原因是因为优化机制 `WHERE` 会在扫描数据时就过滤掉无效的行
- 最后通过 `ORDER BY` 和 `LIMIT` 按平均分获取前十的电影

### Show Me Code
```sql
SELECT sex, m.moviename AS name, avgrate, total
FROM (SELECT 'M' AS sex, movieid, AVG(tr.rate) AS avgrate, COUNT(1) AS total
      FROM t_rating tr
                JOIN (SELECT * FROM t_user WHERE sex = 'M') tu ON tr.userid = tu.userid
      GROUP BY tr.movieid
      HAVING total > 50) a
         LEFT JOIN t_movie m ON a.movieid = m.movieid
ORDER BY avgrate DESC
LIMIT 10;
```

### 运行结果
![](../resource/hive02.png)

## 题目三
> 找出影评次数最多的女士所给出最高分的10部电影的平均影评分，展示电影名和平均影评分（可使用多行SQL）

### 我先思考
- 如果能用连接查询就不要用自查询

### Show Me Code
```sql
WITH top10_movies AS (
  SELECT rate, tr.userid, tr.movieid, tr.times
  FROM t_rating tr
         JOIN (SELECT tu.userid, COUNT(1) AS cnt
               FROM t_rating
                      JOIN (SELECT * FROM t_user WHERE sex = 'F') tu ON t_rating.userid = tu.userid
               GROUP BY tu.userid
               ORDER BY cnt DESC
               LIMIT 1
  ) a ON tr.userid = a.userid
  ORDER BY rate DESC, times DESC
  LIMIT 10)
SELECT tm.moviename AS moviename, c.avgrate
FROM (SELECT tr.movieid, AVG(tr.rate) AS avgrate
      FROM t_rating tr
             JOIN top10_movies top ON tr.movieid = top.movieid
      GROUP BY tr.movieid) c
       LEFT JOIN t_movie tm ON c.movieid = tm.movieid
```
### 运行结果
![](../resource/hive03.png)

基本SQL教程：
- https://www.w3school.com.cn/sql/index.asp
- https://www.liaoxuefeng.com/wiki/1177760294764384

Hive DDL：
- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL


## 附加作业
实现 GeekFile

参考链接
https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide#DeveloperGuide-RegistrationofNativeSerDes
https://github.com/apache/hive/blob/master/contrib/src/java/org/apache/hadoop/hive/contrib/fileformat/base64/Base64TextInputFormat.java
