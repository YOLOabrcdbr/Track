//package com.raphtory.examples.lotr.spouts
//
//import com.raphtory.core.components.spout.Spout
//import scala.collection.mutable
//
//class LOTRSpout extends Spout[String] {
//  val filename = "src/main/scala/com/raphtory/examples/lotr/data/lotr.csv"
//  val fileQueue = mutable.Queue[String]()
//
//  override def setupDataSource(): Unit = {
//    fileQueue++=
//      scala.io.Source.fromFile(filename)
//        .getLines
//  }//no setup
//
//  override def generateData(): Option[String] = {
//    if(fileQueue isEmpty){
//      dataSourceComplete()
//      None
//    }
//    else
//      Some(fileQueue.dequeue())
//  }
//  override def closeDataSource(): Unit = {}//no file closure already done
//}
