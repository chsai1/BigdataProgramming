import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object code3 {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saidivya/Desktop/survey.csv")
    file.registerTempTable("survey")
    val row = file.rdd.take(13).last
    print(row)
  }
}