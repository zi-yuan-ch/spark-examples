package helios.job

import org.apache.spark.sql.{SaveMode, SparkSession}

object MysqlWriteJob {
  def main(args: Array[String]): Unit = {

  }

  private def writeMethod1(): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(MysqlWriteJob.getClass.getName)
      .getOrCreate()
    val size = 1000
    val idDf = spark.range(1, size, 1, numPartitions = 10)
    import org.apache.spark.sql.functions.col
    val df = idDf.withColumn("value", col("id"))
      .withColumnRenamed("id", "name")
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "test02")
      .option("user", "root")
      .option("password", "123456789")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def writeMethod2(): Unit = {

  }

}
