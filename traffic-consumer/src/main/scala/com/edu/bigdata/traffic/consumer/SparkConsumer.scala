package com.edu.bigdata.traffic.consumer

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import com.edu.bigdata.traffic.consumer.util.{PropertyUtil, RedisUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 堵车预测：处理实时数据，消费数据到 redis
  */
object SparkConsumer {

  def main(args: Array[String]): Unit = {
    // 初始化 Spark
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TrafficStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // 设置检查点目录
    ssc.checkpoint("./ssc/checkpoint")

    // 配置 kafka 参数，使用的是 spark 为我们封装的一套操作 kafka coonsumer 的工具包
    val kafkaParam = Map("metadata.broker.list" -> PropertyUtil.getProperty("metadata.broker.list"))

    // 配置 kafka 主题
    val topics = Set(PropertyUtil.getProperty("kafka.topics"))

    // 读取 kafka 主题 中的每一个事件 event
    val kafkaLineDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics)
      .map(_._2) // 由于我们 event 中的键是 null，所以我们需要把值映射出来

    // 解析 json 字符串
    val event = kafkaLineDStream.map(line => { // {"monitor_id":"0001","speed":"038"}
      // 使用 fastjson 来解析当前事件中封装的数据信息，由于该 json 字符串不支持 Scala Map，所以需要先将 json 字符串解析为 Java Map
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]]() {})
      // 将 Java Map 转换成 Scala Map
      import scala.collection.JavaConverters._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap) // Map[String, String] = ("monitor_id" -> "0001", "speed" -> "038")
      lineScalaMap
    })

    // 将每一条数据根据 monitor_id 聚合，聚合每一条数据中的 “车辆速度” 叠加
    // 例如：聚合好的数据形式：(monitor_id, (speed, 1))  ("0001", (038, 1))
    // 最終結果举例：("0001", (1365, 30))
    val sumOfSpeedAndCount = event
      .map(e => (e.get("monitor_id").get, e.get("speed").get)) // ("0001", "038")、("0001", "048")、("0002", "015")
      .mapValues(s => (s.toInt, 1)) // ("0001", (038, 1))、("0001", (048, 1))、("0002", (015, 1))
      .reduceByKeyAndWindow( // reduce 表示从左边开始执行将得到的结果返回给第一个参数
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2),
      Seconds(60), // 滑动窗口大小 60 秒，误差最大 59 秒，即上一分钟的数据当成下一分钟的数据来用了。
      Seconds(60)) // 滑动步长 60 秒，对我们实际建模的影响忽略不计，因为：实际中，不可能1分钟内就造成大量拥堵，或者堵车不可能1分钟之内就缓解了！！！后面建模的时候会进行线性滤波。

    // 定义 redis 数据库中的数据库索引 index
    val dbIndex = 1
    // 将采集到的数据，按照每分钟放置于redis 中，将用于后边的数据建模
    sumOfSpeedAndCount.foreachRDD(rdd => {
      rdd
        .foreachPartition(partitionRecords => {
          partitionRecords
            .filter((tuple: (String, (Int, Int))) => tuple._2._1 > 0) // 过滤掉元组数据中的速度小于0的数据
            .foreach(pair => {
            // 开始取出这 60 秒的 windows 中所有的聚合数据进行封装，准备存入 redis 数据库
            val jedis = RedisUtil.pool.getResource

            val monitorId = pair._1
            val sumOfCarSpeed = pair._2._1
            val sumOfCarCount = pair._2._2

            // 模拟数据为实时流入
            // 两种情况：
            // 1、数据生产时，会产生时间戳字段，流入到 kafka 的事件中
            // 2、数据消费时，数据消费的时间，就当做数据的生产时间（会有一些小小误差），本业务选择这种方式

            val dateSDF = new SimpleDateFormat("yyyyMMdd") // 用于 redis 中的 key
            val hourMinuteSDF = new SimpleDateFormat("HHmm") // 用于 redis 中的 fields

            val currentTime = Calendar.getInstance().getTime

            val dateTime = dateSDF.format(currentTime) // 20190528
            val hourMinuteTime = hourMinuteSDF.format(currentTime) // 1617

            // 选择存入的数据库
            jedis.select(dbIndex)
            jedis.hset(dateTime + "_" + monitorId, hourMinuteTime, sumOfCarSpeed + "_" + sumOfCarCount)

            println(dateTime + "_" + monitorId, hourMinuteTime, sumOfCarSpeed + "_" + sumOfCarCount)

            // RedisUtil.pool.returnResource(jedis) // 老的 API
            jedis.close() // 新的 API
          })
        })
    })

    // Spark 开始工作
    ssc.start()
    ssc.awaitTermination()
  }
}

// 复习 Scala 中 Map 的取值方式：

// 方式1-使用 map(key)
//   1、如果 key 存在，则返回对应的值。
//   2、如果 key 不存在，则抛出异常 [java.util.NoSuchElementException]。
//   3、在 Java 中，如果 key 不存在则返回 null。
// 方式2-使用 contains 方法检查是否存在 key
//  使用 containts 先判断再取值，可以防止异常，并加入相应的处理逻辑。
//   1、如果 key 存在，则返回 true。
//   2、如果 key 不存在，则返回 false。
// 方式3-使用 map.get(key).get 取值
//   1、如果 key 存在，则 map.get(key) 就会返回 Some(值)，然后 Some(值).get 就可以取出。
//   2、如果 key 不存在，则 map.get(key) 就会返回 None。
// 方式4-使用 map.getOrElse(key, defaultvalue) 取值
//   底层是：def getOrElse[V1 >: V](key: K, default: => V1)
//   1、如果 key 存在，则返回 key 对应的值。
//   2、如果 key 不存在，则返回默认值。在 java 中底层有很多类似的操作。
// 如何选择取值方式建议
//   如果我们确定 map 有这个 key，则应当使用 map(key)，速度快。
//   如果我们不能确定 map 是否有 key，而且有不同的业务逻辑，使用 map.contains() 先判断再加入逻辑。
//   如果只是简单的希望得到一个值，使用 map4.getOrElse("ip", "127.0.0.1")