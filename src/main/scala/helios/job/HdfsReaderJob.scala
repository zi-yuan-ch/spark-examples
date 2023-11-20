package helios.job

import org.apache.spark.sql.SparkSession

object HdfsReaderJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(HdfsReaderJob.getClass.getName)
      .getOrCreate()
    spark.read
      .option("inferSchema", true)
      .parquet("hdfs://172.26.0.129:8220/")
  }

  private def readFromHdfs(): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(HdfsReaderJob.getClass.getName)
      .getOrCreate()
    val df = spark.read
      .option("inferSchema", true)
      .csv("hdfs://localhost:9000/spark/tmp/t1.csv")
    df.show()
  }
}
