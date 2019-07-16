import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.sql.functions.{count, sum}

object graph {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saidivya\\Documents\\GitHub\\BigdataProgramming\\Spark\\ICP5\\source\\Datasets\\201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\saidivya\\Documents\\GitHub\\BigdataProgramming\\Spark\\ICP5\\source\\Datasets\\201508_station_data.csv")

// Printing the Schema
    trips_df.printSchema()
    station_df.printSchema()

// Temp View

    trips_df.createOrReplaceTempView("Trips")

    station_df.createOrReplaceTempView("Stations")

//concatenation
    val concat = spark.sql("select concat(lat,long) from Stations").show()
    val nstation = spark.sql("select * from Stations")
    val ntrips = spark.sql("select * from Trips")
//duplicates and renaming and creating vertices
    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()
//duplicates and renaming and creating edges
    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")
//creating graph with above vertex and edges
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)//

//showing vertices
    stationGraph.vertices.show()
//showing edges
    stationGraph.edges.show()
//indegree
    val inDeg = stationGraph.inDegrees
    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)
//out degree
    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)


    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))

 //motifs

    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")

    motifs.show()

//BONUS-vertex degree
    val srcCount = trips_df.distinct.groupBy("Start Station")
      .agg(count("*").alias("connecting_count"))
      .withColumnRenamed("Start Station", "id")

    val dstCount = station_df.distinct.groupBy("name")
      .agg(count("*").alias("connecting_count"))
      .withColumnRenamed("name", "id")

    val degrees = srcCount.union(dstCount)
      .groupBy("id")
      .agg(sum("connecting_count").alias("degree"))
    degrees.sort("id").show(5, false)

//most common destinations in the dataset from location to location
    tripEdges.createOrReplaceTempView("trip")
    val commondest = spark.sql("select dst,count(dst) from trip group by dst  limit 3").show()

//highest ratio of in degrees but fewest out degrees
inDeg.createOrReplaceTempView("in")
    outDeg.createOrReplaceTempView("out")

    val bonus4 = spark.sql("select i.id,i.inDegree,o.outDegree from in i join out o on i.id=o.id")
    bonus4.createOrReplaceTempView("bonus4")
    val bonus5 = spark.sql("select id from bonus4 order by inDegree asc,outDegree desc limit 1").show()

//output to a file
    stationGraph.vertices.write.csv("C:\\Users\\saidivya\\Desktop\\j.csv")

    stationGraph.edges.write.csv("C:\\Users\\saidivya\\Desktop\\i.csv")


  }
}