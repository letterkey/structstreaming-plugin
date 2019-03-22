import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * Created by yinmuyang on 19-3-13 14:13.
  */
object TestMysqlSink {

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("kafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //生成模拟数据
    val rateData = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond",10)
      .load

    /**format指定为mysql**/
    rateData.writeStream.format("mysql")
      .option("checkpointLocation", "/tmp/demo-checkpoint")
      .option("username","root")
      .option("password","root")
      .option("table","mysql_sink")
      .option("jdbcUrl","jdbc:mysql://localhost:3306/test")
      .start

    spark.streams.awaitAnyTermination()
  }

}
