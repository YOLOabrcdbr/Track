package analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
import scala.collection.mutable.Map
import scala.collection.mutable.Set

class FindPattern(startTime: Long, endTime: Long, threshold: Int, fileOutput: String) extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        // get inbound transactions within [startTime, endTime]
        val inEdges = vertex.explodeInEdges(startTime, endTime)

        // inLists : key -> transaction value; value -> list of account address
        var inLists: Map[String, Set[String]] = Map()
        for (inEdge <- inEdges) {
          val value: String = inEdge.getPropertyValue("value") match {
            case None => "0" //Or handle the lack of a value another way: throw an error, etc.
            case Some(s: String) => s //return the string to set your value
          }
          val address: String = inEdge.src().toString

          if (inLists.contains(value) & value != "0") {
            val list = inLists(value) + address
            inLists.update(value, list)
          }
          else {
            inLists.update(value, Set(address))
          }
        }

        // outputs a string
        var finalOutput: String = ""
        for (value <- inLists.keys) {
          // length of the list of accounts
          val cnt = inLists(value).size


          // set a threshold
          if (cnt > threshold) {
            finalOutput = finalOutput + value + " : \n"
            for (address <- inLists(value)) {
              finalOutput = finalOutput + address + ", "
            }
            finalOutput = finalOutput + "\n" + cnt + "******\n"
          }

        }


        if (finalOutput.length == 0) {
          vertex.setState("fork-merge", 0)
          vertex.setState("output", "NAN")
        }
        else {
          vertex.setState("fork-merge", 1)
          vertex.setState("output", finalOutput)
        }

      //        Row(vertex.getProperty("address"), "\n*********** Merge- request Info:\n"+output.toString()+"\n########\n")
    })
      .select(vertex => Row(
        vertex.getPropertyOrElse("address", "unknown"),
        vertex.getStateOrElse("fork-merge", -1),
        vertex.getStateOrElse("output", " xx ")
      )
      )
      .filter(row => row.get(1) == 1)
      .writeTo(fileOutput)
  }
}

object FindPattern {
  def apply(startTime: Long = 1589939000,
            endTime: Long = 1589952842,
            threshold: Int = 20,
            fileOutput: String = "/tmp")
  = new FindPattern(startTime, endTime, threshold, fileOutput)
}
