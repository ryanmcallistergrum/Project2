import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scalaj.http.Http
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
    //lat long
    val weather = ujson.read(Http("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/" +
      "38.9697,-77.385/2020-10-01" +  // entering lat,long,date, in respective order
      "?key=3QBYMGNC37EZLURWFBP6EXDCG").asString.body)
    val temp = weather("days").toString()
    import spark.implicits._
    val tempTable = spark.read.json(Seq(temp).toDS)
    tempTable.select("datetime","tempmax","tempmin").show()
    spark.close()
  }
}
