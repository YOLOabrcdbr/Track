package analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
import scala.math.BigInt

/**
 * Description
 * A tainting/infection algorithm for a directed graph. Given the start node(s) and time
 * this algorithm will spread taint/infect any node which has received an edge after this time.
 * The following node will then infect any of its neighbours that receive an edge after the
 * time it was first infected. This repeats until the latest timestamp/iterations are met.
 * *
 * Parameters
 * startTime Long : Time to start spreading taint
 * infectedNodes Set[Long] : List of nodes that will start as tainted
 * stopNodes Set[Long] : If set, any nodes that will not propogate taint
 * output String : Path where the output will be written
 * *
 * Returns
 * Infected ID (Long) : ID of the infected vertex
 * Edge ID (Long) : Edge that propogated the infection
 * Time (Long) : Time of infection
 * Infector ID (Long) : Node that spread the infection
 **/
class Taint(startTime: Long, infectedNodes: Set[Long], taintMethod: String, output: String) extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      // the step functions run on every single vertex ONCE at the beginning of the algorithm
      .step({
        // for each vertex in the graph
        vertex =>
          // check if it is one of our infected nodes

          if (infectedNodes contains vertex.ID) {
            // set its state to tainted
            // "tainted", edge ID, timestamp, value, vetex ID
            val result = List(("tainted", -1, startTime, BigInt(0), "startPoint"))
            // set this node as the beginning state
            vertex.setState("taintStatus", true)
            vertex.setState("taintHistory", result)
            vertex.setState("taintStorage", BigInt(0))
            // tell all the other nodes that it interacts with, after starTime, that they are also infected
            // but also send the properties of when the infection happened
            val outEdges = vertex.explodeOutEdges(after = startTime)

            // for every edge/transaction this node made after the time

            for (edge <- outEdges) {
              // get all the transactions that this node sent after startTime
              if (edge.getTimestamp() >= startTime) {
                val value: BigInt= edge.getPropertyValue("value") match {
                  case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
                  case Some(s: String) => BigInt(s) //return the string to set your value
                }

                edge.send(Tuple5(
                  "tainted",
                  edge.ID(),
                  edge.getTimestamp(),
                  value,
                  vertex.ID()
                ))
              }
            }
          }
      })
      .iterate({
        vertex =>
          // check if any node has received a message
          // obtain the messages as a set
          // "tainted", edge ID, timestamp, value, vetex ID
          val newMessages = vertex.messageQueue[(String, Long, Long, BigInt, Long)].map(item => item).distinct
          // obtain the min time it was infected
          val infectionTime = newMessages.map(item => item._3).min
          val status = newMessages.map(item => item._1).distinct
          val infectionSum: BigInt= newMessages.map(item => item._4.asInstanceOf[BigInt]).sum
//          val infectionAmount= newMessages.map(item => item._4.asInstanceOf[Number].longValue)
//
//          var infectionSum: Long = 0
//          for(value <- infectionAmount){
//            infectionSum = infectionSum + value
//          }
          // check if any of the newMessages are the keyword tainted
          if ((status contains "tainted") & (infectionSum != BigInt(0))) {
            // check if it was previous tainted
            // if not set the new state, which contains the sources of taint
            if (vertex.getOrSetState("taintStatus", false) == false ) {
              vertex.setState("taintHistory", newMessages)
              vertex.setState("taintStatus", true)
            }
            else {
              // otherwise set a brand new state, first get the old txs it was tainted by
              val oldState: List[(String, Long, Long, BigInt, Long)] = vertex.getState("taintHistory")
              // add the new transactions and old ones together
              val newState = List.concat(newMessages, oldState).distinct
              // set this as the new state
              vertex.setState("taintHistory", newState)
            }

            val oldStorage: BigInt = vertex.getStateOrElse("taintStorage", BigInt(0))
            var newStorage: BigInt = oldStorage + infectionSum


            val outEdges = vertex.explodeOutEdges(after = infectionTime)

            // focus on node: mark the node as tainted despite of the amount
            if (taintMethod == "original") {
              for (edge <- outEdges) {
                edge.send(Tuple5(
                  "tainted",
                  edge.ID(),
                  edge.getTimestamp(),
                  BigInt(0),
                  vertex.ID()
                ))
              }
              newStorage = BigInt(0)
            }

            // taint all transactions with same fraction( tainted value= transfer value * fracation)
            else if (taintMethod == "Haircut") {
              // calculate out sum
              var outSum: BigInt = BigInt(0)
              for (edge <- outEdges) {
                val value: BigInt = edge.getPropertyValue("value") match {
                  case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
                  case Some(s: String) => BigInt(s) //return the string to set your value
                }
                outSum = outSum + value
              }
              // has out transaction
              if(outSum != BigInt(0)){
                val fraction: BigDecimal = BigDecimal(newStorage) / BigDecimal(outSum)
                for (edge <- outEdges) {
                  val value: BigInt= edge.getPropertyValue("value") match {
                    case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
                    case Some(s: String) => BigInt(s) //return the string to set your value
                  }

                  var newValue: BigInt = BigInt(0)
                  // if total out is bigger than total in
                  if(fraction < BigDecimal(1)){
                    newValue = (BigDecimal(value) * fraction).toBigInt()
                  }
                  // else: account balance not 0
                  else{
                    newValue = value
                  }
                  edge.send(Tuple5(
                    "tainted",
                    edge.ID(),
                    edge.getTimestamp(),
                    newValue,
                    vertex.ID()
                  ))
                }
                if(fraction < BigDecimal(1)){
                  newStorage = BigInt(0)
                }
                else{
                  newStorage = newStorage - outSum
                }
              }


            }

            else {
              // taint the earlier transactions first
              if (taintMethod == "FIFO") {
                outEdges.sortBy(_.getTimestamp())
              }

              // taint the largest amount of transactions first
              else if (taintMethod == "TIHO") {
                outEdges.sortBy(_.getPropertyValue("value") match {
                  case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
                  case Some(s: String) => BigInt(s) //return the string to set your value
                })
              }
              // they share a same for loop
              for (edge <- outEdges) {
                val value: BigInt= edge.getPropertyValue("value") match {
                  case None => BigInt(0) //Or handle the lack of a value another way: throw an error, etc.
                  case Some(s: String) => BigInt(s) //return the string to set your value
                }
                // current storage >= transfer amount then tain the transferred amount
                if (newStorage != BigInt(0) & newStorage >= value) {
                  edge.send(Tuple5(
                    "tainted",
                    edge.ID(),
                    edge.getTimestamp(),
                    value,
                    vertex.ID()
                  ))
                  newStorage = newStorage - value
                }
                // taint part of the transfer
                else {
                  edge.send(Tuple5(
                    "tainted",
                    edge.ID(),
                    edge.getTimestamp(),
                    newStorage,
                    vertex.ID()
                  ))
                  newStorage = BigInt(0)
                }
              }
            }
            vertex.setState(("taintStorage"),newStorage)
          }
      }, 100, true)
      // get all vertexes and their status
      .select(vertex => Row(
        vertex.ID(),
        vertex.getStateOrElse("taintStatus", false),
        vertex.getStateOrElse("taintStorage", BigInt(0)),
        vertex.getStateOrElse[Any]("taintHistory", "false")
      ))
      // filter for any that had been tainted and save to folder
      .filter(r => r.get(1) == true)
      .explode(row => row.get(3).asInstanceOf[List[(String, Long, Long, Long, Long)]].map(
        // vertext id,  edge Id, value, timestamp, vertex id
        tx => Row(row(0), row(2), tx._2, tx._3, tx._4, tx._5)
      ))
      .writeTo(output)
  }
}

object Taint {
  def apply(startTime: Int, infectedNodes: Set[Long], taintMethod: String, output: String) =
    new Taint(startTime, infectedNodes, taintMethod, output)
}