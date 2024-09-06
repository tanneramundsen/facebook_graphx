import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphOps
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ConnectedComponents

object facebook_graph {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Final Project")
      .master("local[*]")
      .getOrCreate()

    val graphFile = "hdfs://localhost:9000/user/hdadmin/facebook_combined.txt"

    // Load the graph using edgeListFile
    println(s"Loading graph from ${graphFile} ...")
    val canonicalOrientation = true
    val numEdgePartitions = -1 // Use default parallelism
    val edgeStorageLevel = StorageLevel.MEMORY_ONLY // Adjust storage level as needed
    val vertexStorageLevel = StorageLevel.MEMORY_ONLY // Adjust storage level as needed
    val uniGraph: Graph[Int, Int] = GraphLoader.edgeListFile(
      spark.sparkContext,
      graphFile,
      canonicalOrientation,
      numEdgePartitions,
      edgeStorageLevel,
      vertexStorageLevel
    )
    // Graphx assumes directional edges in the (src --> dst) direction
    // when reading from an edgeListFile. We have to explicitly add the
    // reverse direction edges ourselves
    val reverseEdges: EdgeRDD[Int] = uniGraph.reverse.edges
    val biEdges = uniGraph.edges ++ reverseEdges // shorthand for .union()
    val graph: Graph[Int,Int] = Graph(uniGraph.vertices,biEdges)

    // Print basic graph statistics
    val edges: EdgeRDD[Int] = graph.edges
    val vertices: VertexRDD[Int] = graph.vertices
    println(s"Number of vertices: ${vertices.count}")
    println(s"Number of edges: ${edges.count}\n")

    // Identify connected components
    // connectedComponents() returns a graph itself where the vertices are 2Tuples (vertexID, componentVertexID)
    // where vertexID is the vertex ID from the original graph and componentVertexID is the
    // lowest vertexID of the connected component it belongs to
    println("Calculating the graph's connected components...")
    val cc: VertexRDD[Long] = graph.connectedComponents().vertices
    val numComponents: Long = cc.map(vertexPair => vertexPair._2).distinct.count
    if (numComponents > 1) {
      println(s"Graph has $numComponents components.\n")
    } else {
      println("Graph is connected.\n")
    }

    // Calculate Degree Centrality 
    println("Calculating different measures of centrality...")
    val firstVertex: Long = vertices.first._1 // Choose a vertex id

    // Degree Centrality
    val degrees: VertexRDD[Int] = graph.degrees // Use GraphOps for degrees
    val degreeFirst: Long = getNodeDegreeCentrality(firstVertex,graph,degrees) // Get degree centrality of first vertex
    println(s"Degree Centrality of vertex $firstVertex = $degreeFirst")

    // Get shortest path information
    println(s"Getting shortest path information of vertex $firstVertex...")
    val shortestPaths = ShortestPaths
    // shortest path information to vertex
    // This returns a graph where each vertex attribute is a map containing
    // the shortest-path distance to each reachable landmark vertex
    // e.g. vertex (9,Map(3558 -> 7)) indicates that vertexID 9 is 7 hops away from
    // vertexID 3558.
    val shortPathFirst = shortestPaths.run(graph,List(firstVertex))
    for (vid <- List(1,500,1234)) {
      val distanceMap = shortPathFirst.vertices.filter{case (id,map) => id == vid}.first._2
      val distance = distanceMap.get(firstVertex) match {
        case Some(intValue) => intValue
        // Access successful, intValue holds the integer value
        case None => -1
        // Handle the case where the targetID2 is not found in the inner map
        }
      println(s"User $vid is $distance 'friends' away from User $firstVertex")
    }
    println("")
    // Get most distant vertex from firstVertex
    val maxDistance: Array[Int] = shortPathFirst.vertices.map(v => v._2.get(firstVertex).get).top(1)
    val maxDistanceInt: Int = maxDistance(0)
    val furthestPeople = shortPathFirst.vertices.filter{case (id,map) => map.get(firstVertex).get >= maxDistance(0)}.map(v => v._1)
    println(s"The most distant Users from User $firstVertex are $maxDistanceInt 'friends' away. They are:")
    furthestPeople.collect.foreach(value => print(value + " "))
    println("")

    // Page Rank
    println("Calculating PageRank...")
    val pr: Graph[Double,Double] = graph.pageRank(0.001, 0.15)
    // Print top 10 nodes by page rank
    println("Top 10 Users by page rank (userID, pagerank):")
    val topTenPr: Array[(Long,Double)] = pr.vertices.sortBy(-_._2).take(10)
    println(topTenPr.mkString("\n"))
    // Get descriptive statistics on page rank:
    val prValues = pr.vertices.map(v => v._2)
    val min = prValues.min
    val max = prValues.max
    val range = max - min
    val mean = prValues.mean
    println("")
    println("Page Rank descriptive statistics:")
    println(s"Min: $min, Max: $max, Range: $range")
    println(s"Mean: $mean")
    spark.stop()
  }

  def getNodeDegreeCentrality(vid: Long, graph: Graph[Int,Int], degrees: VertexRDD[Int]): Long = {
    val degree: Long = degrees.lookup(vid).head
    return degree
  } 
}
