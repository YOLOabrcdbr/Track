package analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

/**
Description
  The degree of a node in an undirected networks counts the number of neighbours that
  node has. Its weighted degree counts the number of interactions each node has. Finally
  the edge weight counts the number of interactions which have happened across an edge.

Parameters
  path (String) : The path where the output will be written

Returns
  ID (Long) : Vertex ID
  indegree (Long) : The indegree of the node
 **/
class InDegree(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select({
      vertex =>
        val inDegree = vertex.getInNeighbours().size
        Row(vertex.getPropertyOrElse("name", vertex.ID()), inDegree)
    })
      .writeTo(path)
  }
}

object InDegree{
  def apply(path:String) = new InDegree(path)
}