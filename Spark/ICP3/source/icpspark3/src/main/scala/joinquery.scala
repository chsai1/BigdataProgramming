import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object joinquery {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saidivya/Desktop/survey.csv")
    file.registerTempTable("survey")
    file.registerTempTable("survey1")
    val query3 = sqlContext.sql("select s1.C3,s2.C4 from survey s1 join survey1 s2 on s1.C3=s2.C3")
    query3.show()
    query3.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputfile3")

  }
}