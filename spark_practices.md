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

---

## Spark Scala API

**根据订单列表查找总消费金额大于 1000 的人群**

```scala
val trades = sc.textFile("hdfs://.../trades/")
trades.map(_.split('\001')).cache().map(trade => trade(6) -> trade(16).trim.toDouble)
      .reduceByKey(_ + _)
      .filter( e => e._2 > 1000)
      .collect()
```

```
14/11/12 11:47:44 INFO SparkContext: Job finished: collect at <console>:17, took 482.544253647 s
res1: Array[(String, Double)] = Array((spring3_3,1242.3), (hby_001,2664.74), (海之韵秀,1067.6), (cccdasheng,1087.5), (hzsandygu,1580.0), (ciciflp,1338.5), (mch311,1203.03), (ttx2002,1569.23), (白雪1984529,1985.8700000000001), (sally5535,1037.0), (炙焰魅焱,1395.94), (gechenchen198738,1586.38), (lbxstudy,1086.7800000000002), (lih671120,1126.96), (我的手机1198,1856.36), (yaoweixuan1985,2586.53), (q_dan822,1853.260003051758), (youyou_jsy,1440.4400122070322), (魅影馨光,1398.459996948242), (tinnaran,1410.24), (会飞的鱼27101890_68,2487.7400007629394), (喜兔兔99,3431.8600000000006), (安伟娜1,1931.59), (颖颖104,1439.4999938964838), (zhangalbion,1688.7199999999998), (juanjuan12314,6695.389999999999), (嘉禾081210,1969.4199999999998), (freeblue88,1346.7200012207031), (vaniabobo0918,1757.99), (allen201882,1921.74), (shaoqihui77,13...
```

---

![](https://raw.githubusercontent.com/aiyanbo/databricks-spark-knowledge-base-zh-cn/master/images/reduce_by.png)

---

## Spark SQL

**单次购买金额大于 1000 的用户群**

```scala
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

case class Trade(buyer: String, payment: Double)

val trade = sc.textFile("hdfs://.../trades/")
              .map(_.split('\001')).map(t => Trade(t(6), t(16).trim.toDouble))
trade.registerTempTable("trade")

val query = sql("SELECT * FROM trade WHERE payment > 1000")

query.distinct.collect()

```

```
14/11/12 16:22:02 INFO SparkContext: Job finished: collect at SparkPlan.scala:85, took 79.037251155 s
res1: Array[org.apache.spark.sql.Row] = Array([louis_js,1161.0], [michelle505505,1386.0], [comic001,5000.0], [acteve,1880.0], [毓儿爱鱼儿,1026.0], [finalhardis,1429.0], [korinadaddy,8812.0], [拂水作画,5900.0], [璀璨精品日货行,3265.5], [如意算盘2020,5900.0], [惠娜个个,2036.8], [拂水作画,5900.0], [惠娜个个,2036.8], [grantsherman,1128.6], [怕瓦落地釜山,1271.5], [ahuangmao520,1350.0], [爱上小黑瓶,1980.0], [fengxinzi215,1080.0], [daniel661818舒,1249.7], [毓儿爱鱼儿,1458.0], [毓儿爱鱼儿,1400.0], [毓儿爱鱼儿,1361.0], [beiqingduzou,1450.0], [cat9,1075.0], [我有我的lx,1298.0], [恶魔寻宝,1030.75], [angelafr,1152.36], [happy豆子,1065.9], [alexqxj,1080.0], [tb0396565,1148.5], [jojo_boboz,1330.0], [吴家鼎,4995.0], [tb8559779,1102.95], [shenhuaming,1200.0], [songwei880521,1136.0], [zhengxuhong01974,1138.1], [小三北北,1119.2], [sjj_9601,1040.0], [我爱我家1357900,1119.3], [duanwu0...
```

---

## SchemaRDD

```
SQL --mapping--> SchemaRDD
```

**.toString** : SchemaRDD 的结构

```
SchemaRDD[8] at RDD at SchemaRDD.scala:103
== Query Plan ==
== Physical Plan ==
Filter (payment#1 > 1000.0)
 ExistingRdd [buyer#0,payment#1], MapPartitionsRDD[6] at mapPartitions at basicOperators.scala:208
```

**.queryExecution** : Query 执行计划

```
scala> query.queryExecution.analyzed
res12: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = 
Project [buyer#0,payment#1]
 Filter (payment#1 > CAST(1000, DoubleType))
  SparkLogicalPlan (ExistingRdd [buyer#0,payment#1], MapPartitionsRDD[6] at mapPartitions at basicOperators.scala:208)
```

```
scala> query.queryExecution.sparkPlan
res13: org.apache.spark.sql.execution.SparkPlan = 
Filter (payment#1 > 1000.0)
 ExistingRdd [buyer#0,payment#1], MapPartitionsRDD[6] at mapPartitions at basicOperators.scala:208
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
