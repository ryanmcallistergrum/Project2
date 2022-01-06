import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col

object QueryLoader{
  private final val covidData : DataFrame = getSparkSession().read.option("header","true").csv("data/covid_19_data_cleaned.csv")
  private final val maxDeaths : DataFrame = covidData.select(col("Country/Region"),col("Deaths").cast("Int"))
    .groupBy("Country/Region").sum("Deaths")
  private final val popData : DataFrame = getSparkSession().read.option("header","true").csv("data/population_by_country_2020.csv")
  private val deathJoinPop : DataFrame = maxDeaths.join(popData, covidData("Country/Region") === popData("Country"),"inner")
    .select("Country","sum(Deaths)","Population")

  def loadQuery(question : Int) : Unit = {
    throw new NotImplementedError("Method loadQuery not implemented yet!");
  }

  protected def getSparkSession() : SparkSession = {
    val spark : SparkSession = SparkSession
      .builder
      .appName("Covid Analyze App")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  // 1. When was the peak moment of the pandemic during the data period?
  protected def question01() : DataFrame = {
    throw new NotImplementedError("Method question01 not implemented yet!");
  }

  // 2. When was the pandemic's maximums and minimums over the course of the year?
  protected def question02() : DataFrame = {
    throw new NotImplementedError("Method question02 not implemented yet!");
  }

  // 3. How many total confirmed, deaths, and recovered by date?
  protected def question03() : DataFrame = {
    throw new NotImplementedError("Method question03 not implemented yet!");
  }

  // 4. What is the average (deaths / confirmed) by date?
  protected def question04() : DataFrame = {
    throw new NotImplementedError("Method question04 not implemented yet!");
  }

  // 5. What are the 10 max deaths by country?
  protected def question05() : DataFrame = {
    maxDeaths.sort(col("sum(Deaths)").desc)
  }

  // 6. What are the 10 least deaths by country?
  protected def question06() : DataFrame = {
    covidData.sort(col("sum(Deaths)").asc)
  }

  // 7. does temperature at this latitude affect cases?
  protected def question07() : DataFrame = {
    throw new NotImplementedError("Method question07 not implemented yet!");
  }

  // 8. Does the size of hte population affect the number of deaths?
  protected def question08() : DataFrame = {
    deathJoinPop.sort(col("Population").cast("Int").desc)
  }

  // 9. Who is doing hte best and worst in terms of deaths per capita by country?
  protected def question09() : DataFrame = {
    val spark : SparkSession = getSparkSession();
    import spark.implicits._
    val deathCapita : DataFrame = deathJoinPop.withColumn("Deaths Per Capita", $"sum(Deaths)" / $"Population")
    deathCapita.sort(col("Deaths Per Capita").desc)
  }

  // 10. How long do people take to die after a confirmed case?
  protected def question10() : DataFrame = {
    throw new NotImplementedError("Method question10 not implemented yet!");
  }
}