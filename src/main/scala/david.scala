import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object david {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setLogLevel("ERROR")

    // multiple african copies
    //Azerbaijan in multiple continents. Has space in front
    // ms zaandam, Diamond Princess, Others

    val df = spark.read.option("header",true)
      .csv("data/covid_19_data_cleaned.csv").na.fill(0).na.fill("")

    val cont = spark.read.option("header", true)
      .csv("data/continents.csv")
    cont.show(2)

    val cont2 = cont.select(col("Country"))

    val country = df.select(col("`Country/Region`")).distinct()
    country.show(2)

    //    println("our count: ", country.count())
    println("we're missing: ", country.join(cont2, country("Country/Region") === cont2("Country"), "left_anti").count())
    //    println("unique continent countries: ", cont2.join(country, country("Country/Region") === cont2("Country"), "left_anti").count())
    //    println("Joined: ", country.join(cont2, country("Country/Region") === cont2("Country")).count())
    country.join(cont2, country("Country/Region") === cont2("Country"), "left_anti").show(truncate=false)
    //    val df3 = df.select( col("SNo"),to_date(col("ObservationDate"),"MM/dd/yyyy").alias("Date"))
    //
    //    val df4 = df.join(df3,df("SNo") === df3("SNo"),"inner" ).select(df("SNo"), col("Date"),
    //      col("`Province/State`").alias("Province"), col("`Country/Region`").alias("Country"), col("Confirmed"),
    //      col("Deaths"), col("Recovered"))
    //
    //    val df5 = df4.withColumn("Confirmed",col("Confirmed").cast("int")).withColumn(
    //      "Deaths",col("Deaths").cast("int")
    //    ).withColumn(
    //      "Recovered",col("Recovered").cast("int")
    //    )
    //    df5.groupBy("Date").sum("Confirmed", "Deaths", "Recovered").orderBy(col("Date").asc).show(100)
    //df.show(500)

    //df2.groupBy("`Province/State`", "").max("confirmed").show(300)
    /*
    df.createOrReplaceTempView("covid19")
    val countries = spark.sql("SELECT DISTINCT COALESCE(`Country/Region`,'') as country, COALESCE(`Province/State`, '') as province from covid19")
    countries.createOrReplaceTempView("provinces")
    val provinces = spark.sql("SELECT COALESCE(provinces.province, 'unknown') , countries.id as country_id FROM provinces " +
      "LEFT JOIN countries " +
      "ON provinces.country = countries.country")
    provinces.repartition(1).write.csv("src/main/files/uploads/provinces")
    var count = 0

     */
    //spark.sql("DROP TABLE IF EXISTS countries")
    //spark.sql("CREATE TABLE IF NOT EXISTS countries(id int, country string) row format delimited fields terminated by ','")
    //countries.foreach(row => {
    //spark.sql(s"INSERT INTO countries VALUES( $count, '${row.get(0).toString}')")
    //count+=1
    //})


    /*
    spark.sql("DROP TABLE IF EXISTS provinces")
    spark.sql("CREATE TABLE IF NOT EXISTS provinces(id int, province string, country_id int) row format delimited fields terminated by ','")
    provinces.foreach(row => {
      spark.sql(s"INSERT INTO provinces VALUES( $count, '${row.get(0).toString}', '${row.get(1).toString}')")
      count+=1
      print(count)
    })
    println("finished here")

     */


    /*
    spark.sql("SELECT covid19.Sno, to_date(from_unixtime(UNIX_TIMESTAMP(covid19.ObservationDate,'MM/dd/yyyy'))) as `Observation Date`, covid19.Confirmed , covid19.Deaths, covid19.Recovered, countries.id, COALESCE(covid19.`Province/State`, '') FROM countries " +
      "LEFT JOIN covid19 " +
      "ON countries.country = covid19.`Country/Region`").show(1000)
    */

    /*
    spark.sql("drop table if exists covid_data")
    spark.sql("create table IF NOT EXISTS covid_data(id int, date Date, confirmed int, deaths int, recovered int)" +
    " PARTITIONED BY (country int, province string) row format delimited fields terminated by ','")
    spark.sql("INSERT OVERWRITE TABLE covid_data SELECT covid19.Sno, to_date(from_unixtime(UNIX_TIMESTAMP(covid19.ObservationDate,'MM/dd/yyyy'))) as `Observation Date`, covid19.Confirmed , covid19.Deaths, covid19.Recovered, countries.id, COALESCE(covid19.`Province/State`, '') FROM countries" +
      " LEFT JOIN covid19 " +
      "ON countries.country = covid19.`Country/Region`")

     */



  }
}
