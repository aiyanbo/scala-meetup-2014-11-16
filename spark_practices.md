# Spark 探索

---

## 关于数云

主要产品： 电商CRM软件服务

核心功能： 主动营销，商业智能

---

## 主动营销

![](https://raw.githubusercontent.com/aiyanbo/scala-meetup-2014-11-16/master/images/ccms-nodes.png)
<!-- 精准筛选用户 -> 确定营销内容 -> 选择营销渠道 -> 选择营销时间并执行 -> 获得响应数据 -> 生成效果报告 -->

---

# MySQL 遇到的问题

- 几乎无弹性计算能力
- 每天增长的数据已经接近 MySQL 的上限

---

## 探索 Spark

- 解决现有的问题：性能，时效性
- 提供统一的计算平台，适应未来发展

## 用例

**Standalone mode**

- CPUs: 32
- MEM: 96G * 80%
- WORKs: 1
- DATA: 18G
- FILES: 1010

---

## Spark RDDs

### Transformations

从已有的 Dataset 创建一个新的 Dataset，Lazy 计算模式

- map
- filter
- sample
- union
- groupByKey
- reducyeByKey
- ...

### Actions

从 Dataset 经过计算之后返回值

- reduce
- collect
- count
- first
- saveAsTextFile
- ...

### Persistence

- persist
- cache

---

## Spark Scala API 开发新的业务

**根据订单列表查找总消费金额大于 1000 的人群**

```scala
val trades = sc.textFile("hdfs://.../trades/")
trades.map(_.split('\001')).map(trade => trade(6) -> trade(16).trim.toDouble)
      .reduceByKey(_ + _)
      .filter( e => e._2 > 1000)
      .collect()
```

```
14/11/14 15:49:13 INFO SparkContext: Job finished: collect at <console>:13, took 318.286095399 s
res1: Array[(String, Double)] = Array((spring3_3,1242.3), (hby_001,2664.74), (海之韵秀,1067.6), (cccdasheng,1087.5), (hzsandygu,1580.0), (ciciflp,1338.5), (mch311,1203.03), (ttx2002,1569.23), (白雪1984529,1985.8700000000001), (sally5535,1037.0), (炙焰魅焱,1395.94), (gechenchen198738,1586.38), (lbxstudy,1086.7800000000002), (lih671120,1126.96), (我的手机1198,1856.36), (yaoweixuan1985,2586.53), (q_dan822,1853.260003051758), (youyou_jsy,1440.4400122070322), (魅影馨光,1398.459996948242), (tinnaran,1410.24), (会飞的鱼27101890_68,2487.7400007629394), (喜兔兔99,3431.8600000000006), (安伟娜1,1931.59), (颖颖104,1439.4999938964838), (zhangalbion,1688.7199999999998), (juanjuan12314,6695.389999999999), (嘉禾081210,1969.4199999999998), (freeblue88,1346.7200012207031), (vaniabobo0918,1757.99), (allen201882,1921.74), (shaoqihui77,13...
```

---

![](https://raw.githubusercontent.com/aiyanbo/databricks-spark-knowledge-base-zh-cn/master/images/reduce_by.png)

[图片来源](http://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html)

---

## Spark SQL 兼容老的逻辑 - 逐步替换

**根据订单列表查找总消费金额大于 1000 的人群**

```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

case class Trade(buyer: String, payment: Double)

val trade = sc.textFile("hdfs://.../trades/").map(_.split('\001')).map(t => Trade(t(6), t(16).trim.toDouble))

trade.registerTempTable("trade")

val query = sql("SELECT buyer, SUM(payment) FROM trade GROUP BY buyer HAVING SUM(payment) > 1000")

query.collect()

```

```
14/11/14 15:29:34 INFO SparkContext: Job finished: collect at SparkPlan.scala:85, took 139.138759696 s
res2: Array[org.apache.spark.sql.Row] = Array([shiyan0926,8854.139981689454], [轻轻的来了_001,1136.99], [westwood1213,1797.3], [christy800429,1162.1000000000001], [changshengwang_2009,1439.0], [gy0533,1321.0], [挂挂678,1513.26], [zhangshuo310,1173.4], [liuyang6606,1608.100006103516], [zh1989523,3178.6299999999997], [eleven447643,1471.8], [kkpsworld,1246.0], [jcx000822,1003.21], [svfie,1521.6900019073487], [流水红叶11,3631.570005493165], [shihong7002,1608.0], [mi琪小姐,1036.7], [去去小妖,3151.700047302241], [ljzzq2468,3737.91], [kisswhjk120,1848.34], [tb0585777,2292.8099999999995], [李佩珊00361,1972.4199999999998], [香香八戒,1080.85], [bin325,3303.839997329712], [10ve小又,1039.04], [jasmine芳芳,11246.14999206543], [wanghuining12345678910,2099.5], [tb806888_22,1952.15], [qphd2006,9927.059993782046], [rongxiaosun,1510...
```

---

## SchemaRDD

```
SQL --mapping--> SchemaRDD
```

**.toString** : SchemaRDD 的结构

```
query: org.apache.spark.sql.SchemaRDD = 
SchemaRDD[15] at RDD at SchemaRDD.scala:103
== Query Plan ==
== Physical Plan ==
Project [buyer#0,c1#14]
 Filter havingCondition#15
  Aggregate false, [buyer#0], [(SUM(PartialSum#18) > 1000.0) AS havingCondition#15,buyer#0,SUM(PartialSum#19) AS c1#14]
   Exchange (HashPartitioning [buyer#0], 200)
    Aggregate true, [buyer#0], [buyer#0,SUM(payment#1) AS PartialSum#18,SUM(payment#1) AS PartialSum#19]
     ExistingRdd [buyer#0,payment#1], MapPartitionsRDD[4] at mapPartitions at basicOperators.scala:208
```

**.queryExecution** : Query 执行计划

```
scala> query.queryExecution.analyzed
res4: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = 
Project [buyer#0,c1#14]
 Filter havingCondition#15
  Aggregate [buyer#0], [(SUM(payment#1) > CAST(1000, DoubleType)) AS havingCondition#15,buyer#0,SUM(payment#1) AS c1#14]
   SparkLogicalPlan (ExistingRdd [buyer#0,payment#1], MapPartitionsRDD[4] at mapPartitions at basicOperators.scala:208)
```

```
scala> query.queryExecution.sparkPlan
res5: org.apache.spark.sql.execution.SparkPlan = 
Project [buyer#0,c1#14]
 Filter havingCondition#15
  Aggregate false, [buyer#0], [(SUM(PartialSum#18) > 1000.0) AS havingCondition#15,buyer#0,SUM(PartialSum#19) AS c1#14]
   Aggregate true, [buyer#0], [buyer#0,SUM(payment#1) AS PartialSum#18,SUM(payment#1) AS PartialSum#19]
    ExistingRdd [buyer#0,payment#1], MapPartitionsRDD[4] at mapPartitions at basicOperators.scala:208
```

---

## 未来

### 业务发展

- 实现真正的营销闭环
  
  * 上一次的营销效果影响下一次的营销路径

- 更加智能的使用体验

  * 自动计算营销时间
  * 实时的营销效果预测

- 商业智能与营销紧密结合

  * 个性化智能推荐

- MLib 分析订单评价

### 技术发展

- 使用 Spark Streaming 进行流计算
- 使用 Spark MLib 进行相似度分析

---

# Thanks
