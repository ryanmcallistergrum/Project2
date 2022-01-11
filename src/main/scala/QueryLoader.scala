import org.apache.spark.sql.functions.{col, log}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, coalesce, col, count, date_format, lag, lit, round, to_date, when}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import java.io.File

class QueryLoader{
  private final val covidData : DataFrame = getSparkSession().read.option("header","true").csv("data/covid_19_data_cleaned.csv")
  private final val maxDeaths : DataFrame = covidData.select(col("Country/Region"),col("Deaths").cast("Int"))
    .groupBy("Country/Region").sum("Deaths")
  private final val popData : DataFrame = getSparkSession().read.option("header","true").csv("data/population_by_country_2020.csv")
  private val deathJoinPop : DataFrame = maxDeaths.join(popData, covidData("Country/Region") === popData("Country"),"inner")
    .select(col("Country"),col("sum(Deaths)"),col("Population").cast("Int"))
  private final val monthly_data = getMonthly()
  private final val countryByMonth = getCountryMonthly()
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
    countryByMonth.groupBy("Country").max("Deaths")
      .orderBy(col("max(Deaths)").desc)
      .withColumnRenamed("max(Deaths)","Deaths").limit(10)
  }

  // 6. What are the 10 least deaths by country?
  protected def question06() : DataFrame = {
    countryByMonth.groupBy("Country").min("Deaths")
      .orderBy(col("min(Deaths)"))
      .withColumnRenamed("min(Deaths)","Deaths").limit(10)
  }

  // 7. Do confirmed cases have any relationship to the day of the week?
  protected def question07() : DataFrame = {
    val df : DataFrame = covidData.select(
      col("ObservationDate"),
      col("Country/Region"),
      col("Confirmed").cast("long")
    )
    .withColumn("ObservationDate", to_date(col("ObservationDate"), "MM/dd/yyyy"))

    df.groupBy(col("ObservationDate"))
      .sum("Confirmed").orderBy( "ObservationDate").
      withColumnRenamed("sum(Confirmed)", "Confirmed").withColumn("Difference", coalesce(col("Confirmed")-lag("Confirmed", 1)
      .over(Window.partitionBy().orderBy("ObservationDate")), col("Confirmed")))
      .na.fill(0)
      .withColumn("DayOfWeek", to_date(col("ObservationDate"), "MM/dd/yyyy"))
      .withColumn("DayOfWeek", date_format(col("DayOfWeek"), "u"))
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
    throw new NotImplementedError("Method question10 not implemented yet!");
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
  protected def countryByDate(): Unit ={

  }
  protected def getMonthly():DataFrame = {
    var months = covidData.select( col("SNo"),to_date(col("ObservationDate"),"MM/dd/yyyy").alias("Date"))
    months = covidData.join(months,covidData("SNo") === months("SNo"),"inner" ).select(covidData("SNo"), col("Date"),
      col("`Province/State`").alias("Province"), col("`Country/Region`").alias("Country"), col("Confirmed"),
      col("Deaths"), col("Recovered"))
    months = months.withColumn("Confirmed",col("Confirmed").cast("int")).withColumn(
      "Deaths",col("Deaths").cast("int")
    ).withColumn(
      "Recovered",col("Recovered").cast("int")
    )
    months = months.groupBy("Date").sum("Confirmed", "Deaths", "Recovered").orderBy(col("Date").asc).withColumn("Mortality Rate",
      round(col("sum(Deaths)")/col("sum(Confirmed)"), 3)).withColumnRenamed("sum(Deaths)", "Deaths").
      withColumnRenamed("sum(Confirmed)", "Confirmed").withColumnRenamed("sum(Recovered)", "Recovered")
    months = months.select( date_format(col("Date"),"yyyy-MM").alias("Date"), col("Confirmed"), col("Deaths"), col("Recovered"))
    months = months.groupBy("Date").max("Confirmed", "Deaths", "Recovered").orderBy("Date").withColumnRenamed("max(Deaths)", "Deaths").
      withColumnRenamed("max(Confirmed)", "Confirmed").withColumnRenamed("max(Recovered)", "Recovered")

    months = months.withColumn("Mortality Rate", round(col("Deaths")/col("Confirmed"), 3))
    val windowSpec = Window.partitionBy("Date").orderBy(col("Date").asc)
    months = months.withColumn("Spread Rate",round( (col("Confirmed")-lag("Confirmed", 1).over(Window.partitionBy().
      orderBy("Date")))/ lag("Confirmed", 1).over(Window.partitionBy().
      orderBy("Date")), 3 )).withColumn("Difference", coalesce(col("Confirmed")- lag("Confirmed", 1).over(Window.partitionBy().
      orderBy("Date")), col("Confirmed"))).na.fill(0)
    months.withColumn("Increase in Cases", round( (col("Difference"))/ lag("Difference", 1).over(Window.partitionBy().
      orderBy("Date")), 3 ) ).na.fill(0)
    months
  }
  protected def getCountryMonthly():DataFrame={
    val file = new File("data/country_stats_monthly/")
    if (file.exists() && file.isDirectory){
      val list = file.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv")).toList
      var country:DataFrame = null
      list.foreach(l => {
        val country_data_month = getSparkSession().read.option("header", true)
          .csv(l.toString).na.fill(0).na.fill("")
        country = country_data_month.withColumnRenamed("sum(Deaths)", "Deaths").withColumnRenamed("sum(Confirmed)", "Confirmed").
          withColumnRenamed("sum(Recovered)", "Recovered")


      })
      country.withColumn("Confirmed",col("Confirmed").cast("int")).withColumn(
        "Deaths",col("Deaths").cast("int")
      ).withColumn(
        "Recovered",col("Recovered").cast("int")
      )

    }else{
      var n_df = covidData.withColumn("ObservationDate", to_date(col("ObservationDate"),"MM/dd/yyyy"))
      n_df = n_df.withColumn("ObservationDate", date_format(col("ObservationDate"),"yyyy-MM"))
      n_df = n_df.withColumn("Confirmed",col("Confirmed").cast("int")).withColumn(
        "Deaths",col("Deaths").cast("int")
      ).withColumn(
        "Recovered",col("Recovered").cast("int")
      )
      val countries = n_df.select("`Country/Region`").distinct().collect()
      val country_stats = countries.map(row => {
        var country = n_df.where(s"`Country/Region` == '${row.get(0)}'").groupBy("`Province/State`", "ObservationDate").
          max("Confirmed", "Deaths", "Recovered").orderBy("ObservationDate")
        country.withColumnRenamed("max(Deaths)", "Deaths").withColumnRenamed("max(Confirmed)", "Confirmed").
          withColumnRenamed("max(Recovered)", "Recovered").groupBy("ObservationDate").sum("Confirmed", "Deaths", "Recovered").withColumn("Country", lit(row.get(0)) )
      }).reduce((a: DataFrame, b: DataFrame) => a.union(b))
      country_stats.coalesce(1).write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save("data/country_stats_monthly")
      getCountryMonthly()
    }
  }
}