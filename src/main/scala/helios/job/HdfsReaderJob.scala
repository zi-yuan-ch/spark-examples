package helios.job

import org.apache.spark.sql.SparkSession

object HdfsReaderJob {
  def main(args: Array[String]): Unit = {
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
