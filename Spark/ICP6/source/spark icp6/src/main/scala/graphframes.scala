import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object graphframes {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("ALGORITHMS")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("ALGORITHMS")
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


    trips_df.createOrReplaceTempView("Trips")

    station_df.createOrReplaceTempView("Stations")


    val nstation = spark.sql("select * from Stations")

    val ntrips = spark.sql("select * from Trips")

    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")


    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)//

    stationGraph.vertices.show()

    stationGraph.edges.show()


    // Triangle Count

    val stationTraingleCount = stationGraph.triangleCount.run()
    stationTraingleCount.select("id","count").show()

    // Shortest Path
    val shortPath = stationGraph.shortestPaths.landmarks(Seq("Japantown","Santa Clara County Civic Center")).run
    shortPath.show()

    //Page Rank

    val stationPageRank = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()

    //Saving to File
    stationGraph.vertices.write.csv("C:\\Users\\saidivya\\Desktop\\i61.csv")

    stationGraph.edges.write.csv("C:\\Users\\saidivya\\Desktop\\i62.csv")



    // BFS

    //val pathBFS = stationGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 15").run()
    //pathBFS.show()







  }

}