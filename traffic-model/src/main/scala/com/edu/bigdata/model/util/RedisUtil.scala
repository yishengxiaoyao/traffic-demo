package com.edu.bigdata.model.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

// 代码写在半生对象中，这些代码会在类加载的时候，自动的进行初始化
object RedisUtil {
  // 配置 redis 基本连接参数
  val host = "192.168.25.102"
  val port = 6379
  val timeout = 30000

  val config = new JedisPoolConfig

  // 设置连接池允许最大的连接个数
  config.setMaxTotal(200)
  // 设置最大空闲连接数
  config.setMaxIdle(50)
  // 设置最小空闲连接数
  config.setMinIdle(8)

  // 设置连接时的最大等待的毫秒数
  config.setMaxWaitMillis(10000)
  // 设置在获取连接时，检查连接的有效性
  config.setTestOnBorrow(true)
  // 设置在释放连接时，检查连接的有效性
  config.setTestOnReturn(true)

  // 设置在连接空闲时，检查连接的有效性
  config.setTestWhileIdle(true)

  // 设置两次扫描之间的时间间隔毫秒数
  config.setTimeBetweenEvictionRunsMillis(30000)
  // 设置每次扫描的最多的对象数
  config.setNumTestsPerEvictionRun(10)
  // 设置逐出连接的最小时间间隔，默认是 1800000 毫秒 = 30 分钟
  config.setMinEvictableIdleTimeMillis(60000)

  //  连接池
  lazy val pool = new JedisPool(config, host, port, timeout)

  // 释放资源
  lazy val hook = new Thread{ // 钩子函数：执行一些善后操作，正常退出
    override def run() = {
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run())
}
