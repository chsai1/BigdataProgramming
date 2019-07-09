import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object aggregation {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saidivya/Desktop/survey.csv")
    val save1= file
      .write.format("com.databricks.spark.csv")
      .save("/Users/saidivya/Desktop/savedaggregati")
    file.registerTempTable("survey")
    val query3= sqlContext.sql("select COUNT(C1) from survey")
    query3.show()
    val query4= sqlContext.sql("select SUM(C1) from survey")
    query4.show()

  }

}