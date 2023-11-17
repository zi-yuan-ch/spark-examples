package helios.job

import org.apache.spark.sql.SparkSession

object FileReaderJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("lesson1")
      .getOrCreate()
    val dir = "/Users/chenankang/Desktop/helios/project/spark/spark-example"
    val path = s"file://$dir/src/test/resources/test.parquet"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(path)
    println(df.schema.treeString)
    df.show()
  }
}
