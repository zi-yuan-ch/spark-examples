package helios.job

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Random

object HdfsWriteJob {
  def main(args: Array[String]): Unit = {
    val principal = "hdfs/tw-node130@TDH"
    val path = "hdfs://localhost:9000/spark/tmp/t1.csv"
    val spark = SparkSession.builder()
      .master("local")
      .appName(HdfsWriteJob.getClass.getName)
      .getOrCreate()

    val lines = 10000
    val rows: Array[Row] = new Array[Row](lines)
    for (i <- Array.range(0, lines)) {
      val name = s"name_$i"
      val age = Random.nextInt(100) + 1
      val email = s"email_$i@google.com"
      val location = s"please enter your address"
      val row = Row(name, age, email, location)
      rows(i) = row
    }
    val schema = new StructType()
      .add(StructField("name", StringType, true))
      .add(StructField("age", IntegerType, true))
      .add(StructField("email", StringType, true))
      .add(StructField("location", StringType, true))
    val df = spark.createDataFrame(rows.toList.asJava, schema)
    spark.conf()
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)

  }
}
