import org.apache.spark.{ SparkConf, SparkContext }

object secondarysort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    val conf = new SparkConf().setAppName("SECONDARYSORTING").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("sortinput.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { m => ((m(0), m(1)),m(2))}
    println("PAIRS")
    pairsRDD.foreach { println }
    val numReducers = 4;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(k => k))
    println("LIST")


    listRDD.saveAsTextFile("op3");

  }
}