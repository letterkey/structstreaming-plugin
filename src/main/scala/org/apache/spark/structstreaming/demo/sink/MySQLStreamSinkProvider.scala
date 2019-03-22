package org.apache.spark.structstreaming.demo.sink

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by yinmuyang on 19-3-13 14:06.
  */
class MySQLStreamSinkProvider   extends StreamSinkProvider with DataSourceRegister{
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new MySQLSink(parameters, outputMode)
  }

  override def shortName(): String = "mysql"
}
