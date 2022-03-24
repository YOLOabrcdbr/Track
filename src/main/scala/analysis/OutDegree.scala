package analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

/**
Description
  The degree of a node in an undirected network counts the number of neighbors that
  node has. Its weighted degree counts the number of interactions each node has. Finally,
  the edge weight counts the number of interactions that have happened across an edge.

Parameters
  path (String) : The path where the output will be written

Returns
  ID (Long) : Vertex ID
  outdegree (Long) : The outdegree of the node
 **/
class OutDegree(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select({
      vertex =>
        val outDegree = vertex.getOutNeighbours().size
        Row(vertex.getPropertyOrElse("name", vertex.ID()), outDegree)
    })
      .writeTo(path)
  }
}

object OutDegree{
  def apply(path:String) = new OutDegree(path)
}