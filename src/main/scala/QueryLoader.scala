import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
class QueryLoader{
  val spark = SparkSession
    .builder
    .appName("Covid Analyze App")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  //Questions 1-10 Methods
  def question05():DataFrame={
    val coviddata = spark.read.option("header","true").csv("data/covid_19_data_cleaned.csv")
    val maxdeath = coviddata.select(col("Country/Region"),col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
    maxdeath.sort(col("sum(Deaths)").desc)
  }

  def question06():DataFrame={
    val coviddata = spark.read.option("header","true").csv("data/covid_19_data_cleaned.csv")
    val maxdeath = coviddata.select(col("Country/Region"),col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
    maxdeath.sort(col("sum(Deaths)").asc)
  }

  def question08():DataFrame={
    val coviddata = spark.read.option("header","true").csv("data/covid_19_data_cleaned.csv")
    val maxdeath = coviddata.select(col("Country/Region"),col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
    val popdata = spark.read.option("header","true").csv("data/population_by_country_2020.csv")
    val deathJoinPop = maxdeath.join(popdata,coviddata("Country/Region") === popdata("Country"),"inner")
      .select("Country","sum(Deaths)","Population")
    deathJoinPop.sort(col("Population").cast("Int").desc)
  }


  def question09():DataFrame={
    import spark.implicits._
    val coviddata = spark.read.option("header","true").csv("data/covid_19_data_cleaned.csv")
    val maxdeath = coviddata.select(col("Country/Region"),col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
    val popdata = spark.read.option("header","true").csv("data/population_by_country_2020.csv")
    val deathJoinPop = maxdeath.join(popdata,coviddata("Country/Region") === popdata("Country"),"inner")
      .select("Country","sum(Deaths)","Population")
    val deathcapita = deathJoinPop.withColumn("Deaths Per Capita", $"sum(Deaths)" / $"Population")
    deathcapita.sort(col("Deaths Per Capita").desc)

  }

  //To Show methods
  //example
 // question08().show()
}
