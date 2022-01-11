import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, date_format, lag, log, round, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

class QueryLoader{
  private final val covidData : DataFrame = parseCovidData().cache()
  private final val oldCovid : DataFrame = oldParse().cache()
  private final val maxDeaths : DataFrame = covidData.select(col("Country/Region"),col("Deaths").cast("Int"))
    .groupBy("Country/Region").sum("Deaths")
  private final val popData : DataFrame = getSparkSession().read.option("header","true").csv("data/population_by_country_2020.csv")
  private final val weatherData : DataFrame = getSparkSession().read.option("header", "true").csv("data/daily_weather_2020.csv")
  private val deathJoinPop : DataFrame = maxDeaths.join(popData, covidData("Country/Region") === popData("Country"),"inner")
    .select(col("Country"),col("sum(Deaths)"),col("Population").cast("Int"))

  private def parseCovidData(): DataFrame = {
//
//    df.withColumn("Difference", coalesce(col("Confirmed")-lag("Confirmed", 1).over(Window.partitionBy().orderBy("ObservationDate")), col("Confirmed")))
//      .na.fill(0)
//      .groupBy(col("ObservationDate"), col("Country/Region"), col("Province/State"))
//      .sum("Confirmed", "Difference").orderBy("Country/Region", "ObservationDate")
//      .orderBy("Country/Region", "Province/State", "ObservationDate")
    getSparkSession()
    .read.option("header","true")
    .csv("data/covid_19_data_cleaned.csv")
    .select(
      col("SNo").cast("long"),
      to_date(col("ObservationDate"),"MM/dd/yyyy").alias("Date"),
      col("Province/State"),
      col("Country/Region"),
      col("Confirmed").cast("long"),
      col("Deaths").cast("long"),
      col("Recovered").cast("long")
    ).withColumn("Diff", coalesce(col("Confirmed")-lag("Confirmed", 1).over(Window.partitionBy("Province/State").orderBy("Date")), col("Confirmed")))
    .orderBy("SNo")
    .filter(col("Province/State") === "Hunan")
  }


  private def oldParse(): DataFrame = {
    getSparkSession()
      .read.option("header","true")
      .csv("data/covid_19_data_cleaned.csv")
      .select(
        col("SNo").cast("long"),
        to_date(col("ObservationDate"),"MM/dd/yyyy").alias("Date"),
        col("Province/State"),
        col("Country/Region"),
        col("Confirmed").cast("long"),
        col("Deaths").cast("long"),
        col("Recovered").cast("long")
      )
      .filter(col("Province/State") === "Hunan")
  }

  def loadQuery(question : Int) : DataFrame = {
    oldCovid.show(20)
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
      case 11 => question11()
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

  // 7. Does temperature at this latitude affect cases?
  protected def question07() : DataFrame = {
    throw new NotImplementedError("Method question07 not implemented yet!");
  }

  // 8. Does the size of the population affect the number of deaths?
  protected def question08() : DataFrame = {
    deathJoinPop.sort(col("Population").cast("Int").desc)
  }

  // 9. Who is doing the best and worst in terms of deaths per capita by country?
  protected def question09() : DataFrame = {
    val spark : SparkSession = getSparkSession();
    import spark.implicits._
    val deathCapita : DataFrame = deathJoinPop.withColumn("Deaths Per Capita", $"sum(Deaths)" / $"Population")
    deathCapita.sort(col("Deaths Per Capita").desc)
  }

  // 10. How long do people take to die after a confirmed case?
  protected def question10() : DataFrame = {
//    val dateLag = covidData.select(
//      to_date(col("ObservationDate"),"MM/dd/yyyy").alias("Date"),
//      col("Country/Region"),
//      col("Confirmed").cast("long"),
//      col("Deaths").cast("long"),
//      col("Recovered").cast("long")
//    )
//      .withColumn("laggedConfirmed", lag("Confirmed", 1).over(Window.partitionBy().orderBy("Date")))
//    covidData.printSchema()
    covidData


//    df.withColumn("Difference", coalesce(col("Confirmed")-lag("Confirmed", 1).over(Window.partitionBy().orderBy("ObservationDate")), col("Confirmed")))
//      .na.fill(0)
//      .groupBy(col("ObservationDate"), col("Country/Region"), col("Province/State"))
//      .sum("Confirmed", "Difference").orderBy("Country/Region", "ObservationDate")
//      .orderBy("Country/Region", "Province/State", "ObservationDate")
//    var months = covidData.select( col("SNo"),to_date(col("ObservationDate"),"MM/dd/yyyy").alias("Date"))
//    months = df.join(months,df("SNo") === months("SNo"),"inner" ).select(df("SNo"), col("Date"),
//      col("`Province/State`").alias("Province"), col("`Country/Region`").alias("Country"), col("Confirmed"),
//      col("Deaths"), col("Recovered"))
//    months = months.withColumn("Confirmed",col("Confirmed").cast("int")).withColumn(
//      "Deaths",col("Deaths").cast("int")
//    ).withColumn(
//      "Recovered",col("Recovered").cast("int")
//    )
//    months = months.groupBy("Date").sum("Confirmed", "Deaths", "Recovered").orderBy(col("Date").asc).withColumn("Mortality Rate",
//      round(col("sum(Deaths)")/col("sum(Confirmed)"), 3)).withColumnRenamed("sum(Deaths)", "Deaths").
//      withColumnRenamed("sum(Confirmed)", "Confirmed").withColumnRenamed("sum(Recovered)", "Recovered")
//    months = months.select( date_format(col("Date"),"yyyy-MM").alias("Date"), col("Confirmed"), col("Deaths"), col("Recovered"))
//    months = months.groupBy("Date").max("Confirmed", "Deaths", "Recovered").orderBy("Date").withColumnRenamed("max(Deaths)", "Deaths").
//      withColumnRenamed("max(Confirmed)", "Confirmed").withColumnRenamed("max(Recovered)", "Recovered")
//
//    months = months.withColumn("Mortality Rate", round(col("Deaths")/col("Confirmed"), 3))
//    val windowSpec = Window.partitionBy("Date").orderBy(col("Date").asc)
//    months = months.withColumn("Spread Rate",round( (col("Confirmed")-lag("Confirmed", 1).over(Window.partitionBy().
//      orderBy("Date")))/ lag("Confirmed", 1).over(Window.partitionBy().
//      orderBy("Date")), 3 )).withColumn("Difference", coalesce(col("Confirmed")- lag("Confirmed", 1).over(Window.partitionBy().
//      orderBy("Date")), col("Confirmed"))).na.fill(0)
//    months.withColumn("Increase in Cases", round( (col("Difference"))/ lag("Difference", 1).over(Window.partitionBy().
//      orderBy("Date")), 3 ) ).na.fill(0).show()
  }

  // 11. What's the numerical correlation between population and deaths?
  protected def question11() : DataFrame = {
    println("before: ", deathJoinPop.stat.corr("Population", "sum(Deaths)"))
    val modified = deathJoinPop
      .withColumn("sum(Deaths)", log("sum(Deaths)"))
      .withColumn("Population", log("Population"))
    println("modified: ", modified.stat.corr("Population", "sum(Deaths)"))
    deathJoinPop
  }
}