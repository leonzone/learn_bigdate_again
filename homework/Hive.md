## 题目一：

> 展示电影ID为2116这部电影各年龄段的平均影评分

### 代码
```sql
SELECT tu.age, AVG(tr.rate)
FROM t_movie m
         LEFT JOIN t_rating tr ON m.movieid = tr.movieid
         LEFT JOIN t_user tu ON tr.userid = tu.userid
WHERE m.movieid = 2116
GROUP BY tu.age
ORDER BY tu.age;
```

### 运行结果
![](../resource/hive01.png)

## 题目二：

> 找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数

### 代码
```sql
SELECT sex, m.moviename AS name, avgrate, total
FROM (SELECT 'M' AS sex, movieid, AVG(tr.rate) AS avgrate, COUNT(1) AS total
      FROM t_rating tr
               LEFT JOIN (SELECT * FROM t_user WHERE sex = 'M') tu ON tr.userid = tu.userid
      WHERE tu.sex IS NOT NULL
      GROUP BY tr.movieid
      HAVING total > 50) a
         LEFT JOIN t_movie m ON a.movieid = m.movieid
ORDER BY avgrate DESC
LIMIT 10;
```

### 运行结果
![](../resource/hive02.png)

## 题目三

### 代码
```sql
SELECT tm.moviename, avgrate
FROM (SELECT movieid, AVG(rate) AS avgrate
      FROM t_rating
      WHERE movieid IN
            (
                SELECT movieid
                FROM (
                         SELECT rate, tr.userid, tr.movieid
                         FROM t_rating tr
                         WHERE tr.userid IN (
                             SELECT userid
                             FROM (
                                      SELECT userid, cnt
                                      FROM (
                                               SELECT tu.userid, COUNT(1) AS cnt
                                               FROM t_rating
                                                        LEFT JOIN (SELECT * FROM t_user WHERE sex = 'F') tu ON t_rating.userid = tu.userid
                                               WHERE tu.sex IS NOT NULL
                                               GROUP BY tu.userid) a
                                      ORDER BY cnt DESC
                                      LIMIT 1) b
                         )
                         ORDER BY rate DESC
                         LIMIT 10) c)
      GROUP BY movieid) d
         LEFT JOIN t_movie tm ON d.movieid = tm.movieid;
```
### 运行结果
![](../resource/hive03.png)

## 附加作业
