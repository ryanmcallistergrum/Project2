import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{coalesce, col, date_format, lag, log, to_date, when}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

class QueryLoader{
  private final val covidData : DataFrame = initializeData()
  private final val maxDeaths : DataFrame = covidData.select(col("Country/Region"),col("Deaths").cast("Int"))
    .groupBy("Country/Region").sum("Deaths")
  private final val popData : DataFrame = getSparkSession().read.option("header","true").csv("data/population_by_country_2020.csv")
  private val deathJoinPop : DataFrame = maxDeaths.join(popData, covidData("Country/Region") === popData("Country"),"inner")
    .select(col("Country"),col("sum(Deaths)"),col("Population").cast("Int"))
  private final val countryByMonths : DataFrame = countryByMonth()


  def loadQuery(question : Int) : DataFrame = {
    question match {
      case 1 => question01()
      case 2 => question02()
      case 3 => question03()
      case 4 => question04()
      case 5 => question05()
      case 6 => question06()
      case 7 => question07()
      case 8 => question08()
      case 9 => question09()
      case 10 => question10()
    }
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
    countryByMonths
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
    countryByMonths.withColumn("Deaths",col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
      .orderBy(col("sum(Deaths)").desc).limit(10)
  }

  // 6. What are the 10 least deaths by country?
  protected def question06() : DataFrame = {
    countryByMonths.withColumn("Deaths",col("Deaths").cast("Int"))
      .groupBy("Country/Region").sum("Deaths")
      .orderBy(col("sum(Deaths)").asc)
      .filter(col("sum(Deaths)").isNotNull).limit(10)
  }

  // 7. Do confirmed cases have any relationship to the day of the week?
  protected def question07() : DataFrame = {
    val df : DataFrame = covidData.select(
      col("Date"),
      col("Country/Region"),
      col("Confirmed").cast("long")
    )
    .withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))

    df.groupBy(col("Date"))
      .sum("Confirmed").orderBy( "Date").
      withColumnRenamed("sum(Confirmed)", "Confirmed").withColumn("Difference", coalesce(col("Confirmed")-lag("Confirmed", 1)
      .over(Window.partitionBy().orderBy("Date")), col("Confirmed")))
      .na.fill(0)
      .withColumn("DayOfWeek", to_date(col("Date"), "MM/dd/yyyy"))
      .withColumn("DayOfWeek", date_format(col("DayOfWeek"), "E"))
  }

  // 8. What's the numerical correlation between population and deaths?
  protected def question08() : DataFrame = {
    val modified = deathJoinPop
      .withColumn("sum(Deaths)", log("sum(Deaths)"))
      .withColumn("Population", log("Population"))
    modified.sort(col("Population").desc_nulls_last)
  }

  // 9. Who is doing the best and worst in terms of deaths per capita by country?
  protected def question09() : DataFrame = {
    val deathCapita : DataFrame = deathJoinPop.withColumn("deaths_per_capita", (col("sum(Deaths)") / col("Population")).cast(DecimalType(10,10)))
    deathCapita.sort(col("deaths_per_capita").desc_nulls_last)
  }

  // 10. How long do people take to die after a confirmed case?
  protected def question10() : DataFrame = {
    throw new NotImplementedError("Method question10 not implemented yet!");
  }


  protected def countryByMonth(): DataFrame ={
    var n_df = covidData.withColumn("Date", date_format(col("Date"),"yyyy-MM"))
    n_df = n_df.groupBy("Country/Region", "Date").sum("Confirmed", "Deaths", "Recovered")
      .orderBy("Date").filter(col("Date").isNotNull)

    n_df.withColumnRenamed("sum(Deaths)", "Deaths").
      withColumnRenamed("sum(Confirmed)", "Confirmed").withColumnRenamed("sum(Recovered)", "Recovered")

    /*
    .withColumn("sum(Recovered)", when(col("sum(Recovered)") < 0, 0))
      .withColumn("sum(Deaths)", when(col("sum(Deaths)") < 0, 0))
      .withColumn("sum(Confirmed)", when(col("sum(Confirmed)") < 0, 0))
     */



  }
  protected def initializeData(): DataFrame ={
    getSparkSession().read.option("header","true").csv("data/covid_daily_differences.csv")
      .withColumn("Date", to_date(col("Date")))
      .withColumn("Confirmed",col("Confirmed").cast("long"))
      .withColumn("Confirmed",col("Confirmed").cast("int"))
      .withColumn("Deaths",col("Deaths").cast("int"))
      .withColumn("Recovered",col("Recovered").cast("int"))
      .withColumn("Recovered", when(col("Recovered") < 0, 0).otherwise(col("Recovered")))
      .withColumn("Deaths", when(col("Deaths") < 0, 0).otherwise(col("Deaths")))
      .withColumn("Confirmed", when(col("Confirmed") < 0, 0).otherwise(col("Confirmed")))
  }
}