package helios.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, countDistinct, expr}

object CreateDataFrameJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("CreateDataFrameJob")
      .getOrCreate()
    val df1 = spark.createDataFrame(Seq(("张三", 1), ("张三", 5), ("李四", 2), ("王五", 3))).toDF("name", "age")
    val df2 = df1.withColumn("age2", expr("age * 10"))
      .withColumn("age3", col("age") * 100)
      .withColumn("full", concat(col("name"), col("age"), col("age2")))
      .sort(col("age3").desc)
    val df3 = df1.agg(countDistinct("age").alias("cd"))
    val df4 = df1.select("name").distinct()
    df1.show()
    df2.show()
    df3.show()
    df4.show()
  }
}
