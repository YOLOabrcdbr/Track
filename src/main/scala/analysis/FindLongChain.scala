package analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

import scala.collection.mutable.Set


/**
 * Description
 * *
 * Parameters
 * path String : This takes the path of where the output should be written to
 * iter Int : The number of iteration
 * *
 * Returns
 * ID (Long) : This is the ID of the vertex
 * Label (Long) : The ID of the start vertex of this long chain
 * Value(Long) : The value
 *
 **/
class FindLongChain(path: String, iter: Int) extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          val inEdges = vertex.explodeInEdges()

          var msg: String = ""
          // for every in transaction
          for (inEdge <- inEdges) {
            val inValue: String = inEdge.getPropertyValue("value") match {
              case None => "0" //Or handle the lack of a value another way: throw an error, etc.
              case Some(s: String) => s //return the string to set your value
            }
            val timeStamp = inEdge.getTimestamp()
            val dst = inEdge.dst()
            vertex.messageNeighbour(dst, (inValue, timeStamp, 1))

            msg = msg + inValue + ", 1" + "***"
          }
          vertex.setState("chain", msg)
      })
      .iterate({
        vertex =>
          val queue = vertex.messageQueue[(String, Long, Int)]

          // store all the in transaction values
          var vl: Map[String, Int] = Map()

          // a map to select the (value, timestamp) with the maximum length
          var vl_time: Map[(String, Long), Int] = Map()
          for (tuple <- queue) {
            val key: (String, Long) = (tuple._1, tuple._2)
            // existing chain of the same value at the same timestamp
            // is smaller than
            // the tuple
            if (vl_time.getOrElse(key, 0) < tuple._3) {
              vl_time = vl_time ++ Map(key -> tuple._3)
            }

            if (vl.getOrElse(tuple._1, 0) < tuple._3) {
              vl = vl ++ Map(tuple._1 -> tuple._3)
            }
          }


          // out transactions
          val outEdges = vertex.explodeOutEdges()

          // out message
          for (outEdge <- outEdges) {
            val outValue: String = outEdge.getPropertyValue("value") match {
              case None => "0" //Or handle the lack of a value another way: throw an error, etc.
              case Some(s: String) => s //return the string to set your value
            }
            val outTimeStamp = outEdge.getTimestamp()
            val dst = outEdge.dst()

            for (kv <- vl_time) {
              val inValue: String = kv._1._1
              // when value equals
              if (inValue == outValue) {
                val inTimeStamp: Long = kv._1._2

                // when in time is before out time
                if (inTimeStamp < outTimeStamp) {
                  val length = kv._2 + 1

                  // send message
                  vertex.messageNeighbour(dst, (inValue, outTimeStamp, length))

                  // this value is used
                  vl.-(inValue)
                }
              }
            }
          }

          // now vl contains the unused value
          // meaning it is the end of the chain
          var msg: String = ""
          for (kv <- vl) {
            msg = msg + kv._1 + ", " + kv._2.toString + "***"
          }
          vertex.setState("chain", msg)
      }, iterations = iter, executeMessagedOnly = true)
      .select(vertex => Row(vertex.ID(), vertex.getState[String]("chain")))
      .writeTo(path)
  }
}

object FindLongChain {
  def apply(path: String, iter: Int) = new FindLongChain("/tmp", iter)
}
