import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.functions._
object bao{
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

    val coviddata = spark.read.option("header","true").csv("data/covid_19_data_cleaned.csv")
    val maxdeath = coviddata.select(col("Country/Region"),col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
    val popdata = spark.read.option("header","true").csv("data/population_by_country_2020.csv")
    val question8 = maxdeath.join(popdata,coviddata("Country/Region") === popdata("Country"),"inner")
      .select("Country","sum(Deaths)","Population")


    //Question 5 Max 10 deaths by country
    maxdeath.sort(col("sum(Deaths)").desc).show(10)
    //Question 6 Least 10 deaths by country
    maxdeath.sort(col("sum(Deaths)").asc).show(10)
    //Question 8 correlation between deaths and population
    question8.sort(col("Population").cast("Int").desc).show()
    //Question 9 Deaths per Capita worst vs best
    import spark.implicits._
    val deathcapita = question8.withColumn("Deaths Per Capita", $"sum(Deaths)" / $"Population")
    val maxcapita = deathcapita.sort(col("Deaths Per Capita").desc).show()
    val mincapita = deathcapita.sort(col("Deaths Per Capita").asc).show()
    spark.close()




  }
}