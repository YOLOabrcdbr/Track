package analysis
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

import scala.math.{BigInt,BigDecimal}


/**
Description
  Page Rank algorithm ranks nodes depending on their connections to determine how important
  the node is. This assumes a node is more important if it receives more connections from others.
  Each vertex begins with an initial state. If it has any neighbours, it sends them a message
  which is the inital label / the number of neighbours.
  Each vertex, checks its messages and computes a new label based on: the total value of
  messages received and the damping factor. This new value is propogated to all outgoing neighbours.
  A vertex will stop propogating messages if its value becomes stagnant (i.e. has a change of less
  than 0.00001) This process is repeated for a number of iterate step times. Most algorithms should
  converge after approx. 20 iterations.

Parameters
  dampingFactor (Double) : Probability that a node will be randomly selected by a user traversing the graph, defaults to 0.85.
  iterateSteps (Int) : Number of times for the algorithm to run.
  output (String) : The path where the output will be saved. If not specified, defaults to /tmp/PageRank

Returns
  ID (Long) : Vertex ID
  Page Rank (Double) : Rank of the node
 **/
class PersonalizedPageRank(dampingFactor:Double, tendency:Double, startTime:Long, endTime:Long,iterateSteps:Int, output:String) extends  GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        val initLabel= BigDecimal(1.0)
        vertex.setState("pprlabel",initLabel)

        // temporal reasoning
        val outEdges = vertex.explodeOutEdges(after = startTime)
        val inEdges = vertex.explodeInEdges(before = endTime)

        var outSum: BigInt = BigInt(0)
        for (edge <- outEdges) {
          val value: BigInt = edge.getPropertyValue("value") match {
            case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
            case Some(s: String) => BigInt(s) //return the string to set your value
          }
          outSum = outSum + value
        }

        // has out transaction
        if(outSum != BigInt(0)) {
          for (edge <- outEdges) {
            val value: BigInt = edge.getPropertyValue("value") match {
              case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
              case Some(s: String) => BigInt(s) //return the string to set your value
            }
            if (value != BigInt(0)) {
              val fraction: BigDecimal = BigDecimal(value) / BigDecimal(outSum) * BigDecimal(tendency)
              edge.send(fraction)
            }
          }
        }

        var inSum: BigInt = BigInt(0)
        for (edge <- inEdges) {
          val value: BigInt = edge.getPropertyValue("value") match {
            case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
            case Some(s: String) => BigInt(s) //return the string to set your value
          }
          inSum = inSum + value
        }

        // has out transaction
        if(inSum != BigInt(0)) {
          for (edge <- inEdges) {
            val value: BigInt = edge.getPropertyValue("value") match {
              case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
              case Some(s: String) => BigInt(s) //return the string to set your value
            }
            if (value != BigInt(0)) {
              val fraction: BigDecimal = BigDecimal(value) / BigDecimal(inSum) * BigDecimal(1-tendency)
              edge.send(fraction)
            }
          }
        }
    }).
      iterate({ vertex =>
        val vname = vertex.getPropertyOrElse("name",vertex.ID().toString) // for logging purposes
        val currentLabel = vertex.getState[BigDecimal]("pprlabel")

        val queue = vertex.messageQueue[BigDecimal]

        val newLabel = BigDecimal(1 - dampingFactor) + BigDecimal(dampingFactor) * queue.sum
        vertex.setState("pprlabel", newLabel)

        val outEdges = vertex.explodeOutEdges()

        var outSum: BigInt = BigInt(0)
        for (edge <- outEdges) {
          val value: BigInt = edge.getPropertyValue("value") match {
            case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
            case Some(s: String) => BigInt(s) //return the string to set your value
          }
          outSum = outSum + value
        }

        // has out transaction
        if(outSum != BigInt(0)) {
          for (edge <- outEdges) {
            val value: BigInt = edge.getPropertyValue("value") match {
              case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
              case Some(s: String) => BigInt(s) //return the string to set your value
            }
            if (value != BigInt(0)) {
              val fraction: BigDecimal = BigDecimal(value) / BigDecimal(outSum)
              edge.send(fraction)
            }
          }
        }
      }, iterateSteps,false) // make iterate act on all vertices, not just messaged ones
      .select({
        vertex =>
          Row(
            vertex.getPropertyOrElse("name", vertex.ID()),
            vertex.getStateOrElse("pprlabel", -1)
          )
      })
      .writeTo(output)
  }
}

object PersonalizedPageRank{
  def apply(dampingFactor:Double, tendency: Double, startTime:Long, endTime:Long, iterateSteps:Int, output:String) =
    new PersonalizedPageRank(dampingFactor, tendency,startTime, endTime, iterateSteps, output)
}