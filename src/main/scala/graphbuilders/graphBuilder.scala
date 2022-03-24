package graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.generic.messaging._
import com.raphtory.core.model.graph.{ImmutableProperty, LongProperty, Properties, Type}

class graphBuilder extends GraphBuilder[String]{

  override def parseTuple(tuple: String) = {

    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)

    val timeStamp  = fileLine(5).toLong

    val value = fileLine(2)
    val gas = fileLine(3)
    val gas_price = fileLine(4)

    addVertex(timeStamp, srcID, Properties(ImmutableProperty("address",sourceNode)), Type("Account"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("address",targetNode)), Type("Account"))
    addEdge(
      timeStamp,
      srcID,
      tarID,
      Properties(
        ImmutableProperty("value",value),
        ImmutableProperty("gas",gas),
        ImmutableProperty("gas_price",gas_price)
      ),
      Type("Transaction")
    )
  }
}
