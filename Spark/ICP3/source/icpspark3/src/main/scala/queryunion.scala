import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object queryunion {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saidivya/Desktop/survey.csv")
    val save1= file
      .write.format("com.databricks.spark.csv")
      .save("/Users/saidivya/Desktop/savedunion1")
    file.registerTempTable("survey")
    val query1= sqlContext.sql("select * from survey where C13 like '%Yes' union select * from survey where C13 like '%No' order by C3")
    println("Union Query Executed!")
    query1.show()
  }
}