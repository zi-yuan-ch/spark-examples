package helios.job

import org.apache.spark.sql.{SaveMode, SparkSession}

object MysqlReaderJob {
  def main(args: Array[String]): Unit = {
    readMethod1()
    readMethod2()
  }

  private def readMethod1(): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.catalog.mysql_db", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
      .config("spark.sql.catalog.mysql_db.url", "jdbc:mysql://localhost:3306/test?useSSL=false")
      .config("spark.sql.catalog.mysql_db.user", "root")
      .config("spark.sql.catalog.mysql_db.password", "123456789")
      .config("spark.sql.catalog.mysql_db.pushDownLimit", "true")
      .master("local[*]")
      .appName("simple file read")
      .getOrCreate()
    val df = spark.sql("select * from mysql_db.test02")
    df.show()
  }
  private def readMethod2(): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(MysqlReaderJob.getClass.getName)
      .getOrCreate()
    val df = spark.read
      .format("jdbc2")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "test02")
      .option("user", "root")
      .option("password", "123456789")
      .option("pushDownLimit", "true")
      .option("numPartitions", 10)
      .option("lowerBound", 1)
      .option("upperBound", 10000)
      .option("partitionColumn", "value")
      .option("fetchsize", 100)
      .option("batchsize", 1000)
      .option("pushDownLimit", "true")
      .load()
    df.show()
  }


}
