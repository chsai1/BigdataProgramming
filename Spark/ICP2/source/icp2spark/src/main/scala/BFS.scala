import org.apache.spark.{SparkConf, SparkContext}
object BFS {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type V = Int
    type Graph = Map[V, List[V]]
    val g: Graph = Map(1 -> List(2,3,5), 2 -> List(1,3,6), 3 -> List(3,4), 4 -> List(1,3),5 -> List(1,6),6 -> List(1,2))
    //I want this to return results in the different layers that it finds them (hence the list of list of vertex)
    def BFS(start: V, g: Graph): List[List[V]] = {
      def BFS0(elems: List[V],visited: List[List[V]]): List[List[V]] = {
        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newNeighbors.isEmpty)
          visited
        else
          BFS0(newNeighbors, newNeighbors :: visited)
      }
      BFS0(List(start),List(List(start))).reverse
    }
    val bfsresult=BFS(1,g )
    println(bfsresult.mkString(","))

  }
}