package helios.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Random

object HdfsWriteJob {
  def main(args: Array[String]): Unit = {
    val principal = "hdfs/tw-node131@TDH"
    val dir = "/Users/chenankang/Desktop/helios/project/spark/spark-example/"
    val resource = "src/main/resources/"
    val keytab = s"$dir${resource}hdfs.keytab"
    val krb5Conf = s"$dir${resource}krb5.conf"
    //    val path = "hdfs://localhost:9000/spark/tmp/t1.csv"
    val path = "hdfs://172.26.0.129:8220/myjfs/hdfs/t1.csv"
    val spark = SparkSession.builder()
      .master("local")
      .appName(HdfsWriteJob.getClass.getName)
      .config("spark.authenticate", "true")
      .config("hadoop.security.authentication", "kerberos")
      .getOrCreate()

    val lines = 100
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

    // 设置Kerberos认证的Spark配置
    //    spark.sparkContext.hadoopConfiguration.set("spark.authenticate", "true")
    //    spark.sparkContext.hadoopConfiguration.set("spark.yarn.principal", principal)
    //    spark.sparkContext.hadoopConfiguration.set("spark.yarn.keytab", keytab)
    //    spark.sparkContext.hadoopConfiguration.set("spark.yarn.krb5.conf", krb5Conf)
    val conf: Configuration = new Configuration
    conf.set("hadoop.security.authentication", "kerberos")
    //        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    spark.conf.set("hadoop.security.authentication", "kerberos")
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("hadoop.security.authentication", "kerberos")
      .option("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      .csv(path)

  }
}
