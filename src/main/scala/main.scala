object main {
  def main(args:Array[String]): Unit ={
    val query = new QueryLoader
    query.loadQuery(8).show()
  }
}
