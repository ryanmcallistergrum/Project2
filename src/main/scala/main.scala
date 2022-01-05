import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scalaj.http.Http
import org.apache.spark.sql.functions._
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
    spark.sparkContext.setLogLevel("ERROR")
    
    val coviddata = spark.read.option("header","true").csv("files/covid_19_data.csv")
    val maxdeath = coviddata.select(col("Country/Region"),col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
    //Max 10 deaths by country
    maxdeath.sort(col("sum(Deaths)").desc).show(10)
    //Least 10 deaths by country
    maxdeath.sort(col("sum(Deaths)").asc).show(10)


    spark.close()




  }
}
