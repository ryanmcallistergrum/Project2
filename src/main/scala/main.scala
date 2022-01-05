import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, to_date}
object main {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    val spark = SparkSession
      .builder
      .appName("Covid Analyze App")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("header","true")
      .csv("files/time_series_covid_19_confirmed.csv")
    val ans = df.select(col("Country/Region"),col("Province/State"),col("1/20/21").cast("int"))
    val ans2 = ans.sort(col("1/20/21").desc)
    ans2.show(100)



  }
}
