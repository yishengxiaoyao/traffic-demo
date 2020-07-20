package com.edu.bigdata.model

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.edu.bigdata.model.util.RedisUtil
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 堵车预测：建模，不同的卡口不同的模型（函数）
  */
object Train {
  def main(args: Array[String]): Unit = {
    // 写入文件的输出流，将本次评估结果保存到下面这个文件中
    val writer = new PrintWriter(new File("model_train.txt"))

    // 初始化 Spark
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TrafficTrainModel")
    val sc = new SparkContext(sparkConf)

    // 定义 redis 的数据库相关参数
    val dbIndex = 1
    // 获取 redis 连接
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)

    // 设定 目标监测点：你要对哪几个监测点进行建模（本例中对 2 个检测点进行建模）
    val targetMonitorIDs = List("0005", "0015")
    // 取出 目标监测点的相关监测点：算法工程师告诉我们的（本例中我们随意写几个）
    val relationMonitors = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017")
    )

    // 访问 redis 取出 目标监测点的相关监测点 的数据

    // 遍历 目标监测点的相关监测点 的 Map 集合
    targetMonitorIDs.map(targetMonitorID => { // 这个 map 执行 2 次
      // 初始化时间
      // 获取当前时间
      val currentDate = Calendar.getInstance().getTime

      // 格式化 当前时间 为 年月日 对象
      val dateSDF = new SimpleDateFormat("yyyyMMdd")
      // 格式化 当前时间 为 小时分钟数 对象
      val hourMinuteSDF = new SimpleDateFormat("HHmm")

      // 格式化当前时间
      val dateSDFString = dateSDF.format(currentDate) // 20190528

      // 获取 当前目标监测点的相关监测点
      val relationMonitorArray = relationMonitors(targetMonitorID)
      // 根据 当前目标监测点的相关监测点，取出当前时间的所有相关监测点的平均车速
      val relationMonitorInfo = relationMonitorArray.map(relationMonitorID => { // 这个 map 执行 5 次
        (relationMonitorID, jedis.hgetAll(dateSDFString + "_" + relationMonitorID))
        // ("0003", {"0900":"1356_30", "0901":"100_2", ..., "0959":"134_4"})
      })

      // 创建 3 个数组：因为要使用 拟牛顿法(LBFGS)进行建模，该方法需要
      // 第一个数组放 特征因子数据集，
      // 第二个数组放 label 标签向量（特征因子对应的结果数据集），
      // 第三个数组放 前两者之间的关联（即真正的特征向量）
      val dataX = ArrayBuffer[Double]() // 实际的每一分钟的平均车速
      val dataY = ArrayBuffer[Double]() // 第 4 分钟的平均车速

      // 用于存放 特征因子数据集 和 特征因子对应的结果数据集 的映射关系
      val dataTrain = ArrayBuffer[LabeledPoint]()

      // 确定使用多少时间内的数据进行建模（本例中取 1 小时）
      val hours = 1

      // 将时间回退到当前时间的 1 小时之前，时间单位：分钟
      // 遍历 目标监测点的数据（外循环）
      for (i <- Range(60 * hours, 2, -1)) { // 本例中是 60 到 2(不包括2)，步长是 -1，即 60, 59, 58, ..., 5, 4,
        dataX.clear()
        dataY.clear()

        // 遍历 目标监测点的所有相关监测点 的数据（内循环）
        for (index <- 0 to 2) {
          // 当前for循环 的时间 = 当前时间的毫秒数 - 1 个小时的毫秒数 + 0分钟的毫秒数，1分钟的毫秒数，2分钟的毫秒数  （第3分钟作为监督学习的结果向量--label 向量）
          val oneMoment = currentDate.getTime - 60 * i * 1000 + 60 * index * 1000
          // 获取 当前for循环 的时间的小时分钟数
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))

          // 获取当前小时分钟数的数据
          for ((k, v) <- relationMonitorInfo) { // ("0003", {"0900":"1356_30", "0901":"100_2", ..., "0959":"134_4"})

            // hours 个小时前的后 3 分钟的数据，组装到 dataX 中
            if (v.containsKey(oneHM)) { // 判断本次时刻的数据是否存在，如果存在，则取值，否则，则取 -1(表示数据缺失)
              val speedAndCarCount = v.get(oneHM).split("_")
              val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat // 得到当前这一分钟的平均车速
              dataX += valueX
            } else {
              dataX += -59.0F
            }

            // 如果 index == 2，说明已经得到 hours 个小时前的后 3 分钟的数据，并组装到了 dataX 中；如果是目标卡口，则说明下一分钟数据是 label 向量的数据，ze存放 dataY 中
            if (index == 2 && targetMonitorID == k) {
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(nextMoment))
              if (v.containsKey(nextHM)) { // 判断本次时刻的数据是否存在，如果存在，则取值，否则，则不管它（有默认值 0）
                val speedAndCarCount = v.get(nextHM).split("_")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat // 得到第 4 分钟的平均车速
                dataY += valueY
              }
            }

          }
        }

        // 准备训练模型
        // 先将 dataX 和 dataY 映射到一个 LabeledPoint 对象中
        if (dataY.toArray.length == 1) { // 说明结果集中有数据了
          val label = dataY.toArray.head
          val record = LabeledPoint(
            // 因为使用的是 拟牛顿法(LBFGS) 进行建模，该方法需要 特征结果 有几种情况（不能是无穷种情况）
            // label 范围为 0~6（7个类别），越大则道路越通畅
            if (label / 10 < 6) (label / 10).toInt else 6, Vectors.dense(dataX.toArray)
          )
          dataTrain += record
        }
      }

      // 将特征数据集写入到文件中方便查看，至此，我们的特征数据集已经封装完毕
      dataTrain.foreach(record => {
        println(record)
        writer.write(record.toString() + "\r\n")
      })

      // 将特征数据集转为 rdd 数据集
      val rddData = sc.parallelize(dataTrain)
      // 随机封装训练集和测试集
      val randomSplits = rddData.randomSplit(Array(0.6, 0.4), 11L)
      val trainData = randomSplits(0)
      val testData = randomSplits(1)

      if (!rddData.isEmpty()) {
        // 使用训练数据集进行训练模型
        val model = new LogisticRegressionWithLBFGS().setNumClasses(7).run(trainData)

        // 使用测试数据集测试训练好的模型
        val predictAndLabel = testData.map {
          case LabeledPoint(label, feature) =>
            val predict = model.predict(feature)
            (predict, label)
        }

        // 得到当前 目标监测点 的评估值
        val metrics = new MulticlassMetrics(predictAndLabel)
        val accuracy = metrics.accuracy
        println("评估值：" + accuracy)
        writer.write(accuracy.toString + "\r\n")

        // 设置评估阈值，评估值范围为[0.0, 1.0]，越大 model 越优秀，我们保存评估值大于 0 的评估模型
        if (accuracy > 0.6) {
          // 将模型保存到 hdfs 中，并将模型路径保存到 redis 中
          val hdfsPath = "hdfs://hadoop102:9000/traffic/model/" + targetMonitorID + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date
          (currentDate.getTime))
          model.save(sc, hdfsPath)

          jedis.hset("model", targetMonitorID, hdfsPath)
        }
      }
    })

    // 释放 redis 连接
    // RedisUtil.pool.returnResource(jedis) // 老的 API
    jedis.close() // 新的 API
    writer.close()
  }
}
