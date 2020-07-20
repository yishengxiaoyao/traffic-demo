package com.edu.bigdata.traffic.producer.util

import java.util.Properties

object PropertyUtil {
  val properties = new Properties()
  // 加载配置属性
  try {
    val inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties")
    properties.load(inputStream)
  } catch {
    case ex: Exception => println(ex)
  } finally {

  }

  // 定义通过键得到属性值的方法
  def getProperty(key: String): String = properties.getProperty(key)
}
