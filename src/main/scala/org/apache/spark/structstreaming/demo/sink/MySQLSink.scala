package org.apache.spark.structstreaming.demo.sink

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by yinmuyang on 19-3-13 12:05.
  */
class MySQLSink(options:Map[String,String],outputMode: OutputMode) extends Sink{
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    /**从option参数中获取需要的参数**/
    val userName = options.get("userName").orNull
    val password = options.get("password").orNull
    val table = options.get("table").orNull
    val jdbcUrl = options.get("jdbcUrl").orNull
    //data.show()
    //println(userName+"---"+password+"---"+jdbcUrl)
    val pro = new Properties
    pro.setProperty("user",userName)
    pro.setProperty("password",password)
    data.write.mode(outputMode.toString).jdbc(jdbcUrl,table,pro)
  }
}
