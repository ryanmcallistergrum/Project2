import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestQueryLoader extends AnyFlatSpec with should.Matchers {
  object Test extends QueryLoader {
    override def loadQuery(question : Int) : DataFrame = super.loadQuery(question)
    override def getSparkSession() : SparkSession = super.getSparkSession()
    override def question01() : DataFrame = super.question01()
    override def question02() : DataFrame = super.question02()
    override def question03() : DataFrame = super.question03()
    override def question04() : DataFrame = super.question04()
    override def question05() : DataFrame = super.question05()
    override def question06() : DataFrame = super.question06()
    override def question07() : DataFrame = super.question07()
    override def question08() : DataFrame = super.question08()
    override def question09() : DataFrame = super.question09()
    override def question10() : DataFrame = super.question10()
  }

  "loadQuery(Int)" should "load the passed-in query into Zeppelin" in {
    throw new NotImplementedError("loadQuery(Int) test not implemented yet!")
  }

  "getSparkSession()" should "return a SparkSession" in {
    assert(Test.getSparkSession() != null)
  }

  "question01()" should "return a DataFrame showing the peak moment of the pandemic was during the data period" in {
    Test.question01().show(Int.MaxValue, false)
  }

  "question02()" should "return a DataFrame showing when was the pandemic's maximums and minimums over the course of the year" in {
    Test.question02().show(Int.MaxValue, false)
  }

  "question03()" should "return a DataFrame showing the total confirmed, deaths, and recovered by date" in {
    Test.question03().show(Int.MaxValue, false)
  }

  "question04()" should "return a DataFrame showing what is the average (deaths / confirmed) by date" in {
    Test.question04().show(Int.MaxValue, false)
  }

  "question05()" should "return a DataFrame showing what are the 10 max deaths by country" in {
    Test.question05().show(Int.MaxValue, false)
  }

  "question06()" should "return a DataFrame showing what are the 10 least deaths by country" in {
    Test.question06().show(Int.MaxValue, false)
  }

  "question07()" should "return a DataFrame showing whether confirmed cases have any relationship to the day of the week" in {
    Test.question07().show(Int.MaxValue, false)
  }

  "question08()" should "return a DataFrame showing whether population size affects the number of deaths" in {
    Test.question08().show(Int.MaxValue, false)
  }

  "question09()" should "return a DataFrame showing who is doing the best and worst in terms of deaths per capita by country" in {
    Test.question09().show(Int.MaxValue, false)
  }

  "question10()" should "return a DataFrame showing how long do people take to die after a confirmed case" in {
    Test.question10().show(Int.MaxValue, false)
  }
}