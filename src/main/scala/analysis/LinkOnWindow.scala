package analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}


class LinkOnWindow(address: String , startTime: Long , endTime: Long , fileOutput:String) extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select({
      vertex=>

        // get outbound and inbound transactions within [startTime, endTime]
//        val outEdges = vertex.getOutEdges(startTime, endTime)
        val outEdges = vertex.explodeOutEdges(startTime, endTime)
//        val inEdges = vertex.getInEdges(startTime, endTime)
        val inEdges = vertex.explodeInEdges(startTime, endTime)

        // List to store transaction IDs
//        var outEdgeIDs : List[Long] = List()
//        var inEdgeIDs : List[Long] = List()

        var inResults: List[Map[String, String]] = List()
        var outResults: List[Map[String, String]] = List()
//        var inResults: List[Any] = List()
//        var outResults: List[Any] = List()
        // traverse to get IDs
        outEdges.foreach(f = edge => {
          val edgeInfo = Map("ID"-> edge.ID().toString,
            "value" -> edge.getPropertyValue("value").toString,
            "timestamp" -> edge.getTimestamp().toString)
//          val edgeInfo = edge.ID()
          outResults = outResults :+ edgeInfo
        })
        inEdges.foreach(edge => {
          val edgeInfo = Map("ID"-> edge.ID().toString,
            "value" -> edge.getPropertyValue("value").toString,
            "timestamp" -> edge.getTimestamp().toString)
//          val edgeInfo = edge.ID()
          inResults = inResults :+ edgeInfo
        })

        // concat IDs to string
        val outString = outResults.mkString("\n")
        val inString = inResults.mkString("\n")
        val vertexAddress = vertex.getPropertyOrElse("address", vertex.ID().toString)

        //
        Row(vertexAddress,startTime, endTime, "\n*********** Out Bound Links Info:\n"+outString, "\n*********** In Bound Links Info:\n"+inString+"\n########\n")
    })
      .filter(row => row.get(0) == address)
      .writeTo(fileOutput)
  }
}

object LinkOnWindow{
  def apply(address: String,
            startTime: Long ,
            endTime: Long,
            fileOutput:String)
  = new LinkOnWindow(address,startTime,endTime, fileOutput)
}
