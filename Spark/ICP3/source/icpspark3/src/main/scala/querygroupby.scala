import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object querygroupby {
  def main(args: Array[String]) {
    setProperty("hadoop.home.dir", "C:\\winutils\\")
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/saidivya/Desktop/survey.csv")
    val save1= file
      .write.format("com.databricks.spark.csv")
      .save("/Users/saidivya/Desktop/savedgroupby1")
    file.registerTempTable("survey")
    val query2= sqlContext.sql("select C7,count(*) from survey group by C7")
    println("GROUPBY Query Executed!")
    query2.show()

  }
}