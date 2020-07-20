package com.edu.bigdata.prediction

import java.text.SimpleDateFormat
import java.util.Date

import com.edu.bigdata.prediction.util.RedisUtil
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 堵车预测：根据训练出来的模型进行堵车预测
  */
object Prediction {
  def main(args: Array[String]): Unit = {
    // 初始化 Spark
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TrafficPrediction")
    val sc = new SparkContext(sparkConf)

    // 时间设置：为了拼凑出 redis 中的 key 和 field 的字段

    // 格式化 时间 为 年月日 对象
    val dateSDF = new SimpleDateFormat("yyyyMMdd")
    // 格式化 时间 为 小时分钟数 对象
    val hourMinuteSDF = new SimpleDateFormat("HHmm")

    // 2019-05-29 13:00
    val userSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm")

    // 定义用户传入的日期：想要预测是否堵车的日期
    val inputDateString = "2019-05-29 10:29"
    val inputDate = userSDF.parse(inputDateString)

    // 得到 redis 中的 key
    val dateSDFString = dateSDF.format(inputDate) // 20180529

    val dbIndex = 1
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)

    // 想要预测的监测点
    // 设定 目标监测点：你要对哪几个监测点进行建模（本例中对 2 个检测点进行建模）
    val targetMonitorIDs = List("0005", "0015")
    // 取出 目标监测点的相关监测点：算法工程师告诉我们的（本例中我们随意写几个）
    val relationMonitors = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017")
    )

    // 遍历 目标监测点的相关监测点 的 Map 集合
    targetMonitorIDs.map(targetMonitorID => { // 这个 map 执行 2 次
      // 获取 当前目标监测点的相关监测点
      val relationMonitorArray = relationMonitors(targetMonitorID)
      // 根据 当前目标监测点的相关监测点，取出当前时间的所有相关监测点的平均车速
      val relationMonitorInfo = relationMonitorArray.map(relationMonitorID => { // 这个 map 执行 5 次
        (relationMonitorID, jedis.hgetAll(dateSDFString + "_" + relationMonitorID))
        // ("0003", {"0900":"1356_30", "0901":"100_2", ..., "0959":"134_4"})
      })

      // 装载目标时间点之前的 3 分钟的历史数据
      val dataX = ArrayBuffer[Double]() // 实际的每一分钟的平均车速

      // 组装数据
      for (index <- Range(3, 0, -1)) {
        val oneMoment = inputDate.getTime - 60 * index * 1000
        val oneHM = hourMinuteSDF.format(new Date(oneMoment)) // 1257

        for ((k, v) <- relationMonitorInfo) {
          if (v.containsKey(oneHM)) {
            val speedAndCarCount = v.get(oneHM).split("_")
            val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
            dataX += valueX
          } else {
            dataX += -59.0F
          }
        }
      }

      // 加载模型
      val modelPath = jedis.hget("model", targetMonitorID)
      val model = LogisticRegressionModel.load(sc, modelPath)

      // 进行预测
      val predict = model.predict(Vectors.dense(dataX.toArray))

      // 打印展示
      println(targetMonitorID + "，堵车评估值：" + predict + "，是否通畅：" + (if (predict > 3) "通畅" else "拥堵"))

      // 结果保存
      jedis.hset(inputDateString, targetMonitorID, predict.toString)
    })

    // 释放 redis 连接
    // RedisUtil.pool.returnResource(jedis) // 老的 API
    jedis.close() // 新的 API
  }
}
