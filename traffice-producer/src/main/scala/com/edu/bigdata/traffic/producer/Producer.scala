package com.edu.bigdata.traffic.producer

import java.text.DecimalFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import com.edu.bigdata.traffic.producer.util.PropertyUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * 模拟产生数据，同时把数据实时的发送到 kafka
  * 随机产生 监测点id 以及 速度
  * 序列化为 json
  * 发送给 kafka
  */
object Producer {
  def main(args: Array[String]): Unit = {
    // 读取配置文件信息
    val properties = PropertyUtil.properties
    // 创建 kafka 生产者对象
    val producer = new KafkaProducer[String, String](properties)

    // 模拟产生实时数据，单位为：秒
    var startTime = Calendar.getInstance().getTimeInMillis() / 1000

    // 数据模拟，堵车状态切换的周期单位为：秒
    val trafficCycle = 300

    val df = new DecimalFormat("0000")
    // 开始不停的实时产生数据
    while (true) {
      // 模拟产生监测点 id：1~20
      val randomMonitorId = df.format(Random.nextInt(20) + 1)
      // 模拟车速
      var randomSpeed = "000"

      // 得到本条数据产生时的当前时间，单位为：秒
      val currentTime = Calendar.getInstance().getTimeInMillis() / 1000
      // 每 5 分钟切换一次公路状态
      if (currentTime - startTime > trafficCycle) {
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(16))
        if (currentTime - startTime > trafficCycle * 2) {
          startTime = currentTime
        }
      } else {
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(31) + 30)
      }

      // 该 Map 集合用于存放生产出来的数据
      val jsonmMap = new java.util.HashMap[String, String]()
      jsonmMap.put("monitor_id", randomMonitorId)
      jsonmMap.put("speed", randomSpeed)

      // 因为 kafka 是基于事件的，在此，我们每一条产生的数据都序列化为一个 json 事件

      val event = JSON.toJSON(jsonmMap)

      // 发送事件到 kafka 集群中
      producer.send(new ProducerRecord[String, String](PropertyUtil.getProperty("kafka.topics"), event.toString))

      Thread.sleep(500)

      // 测试
      // println("监测点id：" + randomMonitorId + "," + "车速：" + randomSpeed)
      println(event)
    }
  }
}
