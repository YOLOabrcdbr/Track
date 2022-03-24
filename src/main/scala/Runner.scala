import com.raphtory.algorithms.{
  ConnectedComponents,
  GraphState,
  TriangleCount,
  twoHopNeighbour,
  GenericTaint,
PageRank
}
import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.spouts.FileSpout
import graphbuilders.graphBuilder
import analysis.{
  FindPattern,
  InDegree,
  LinkOnWindow,
  OutDegree,
  FindLongChain,
  FindForkMerge,
  Taint,PersonalizedPageRank
}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

object Runner extends App {

  // data_20
  // max = 1590010475
//                    val source  = new FileSpout("src/main/scala/data/","data_trimmed_20.csv")

  // data_10
  // max = 1590078477
//           val source  = new FileSpout("src/main/scala/data/","data_trimmed_10.csv")

  // data_100
  // min = 1589939554
  // max = 1589952842
  val source = new FileSpout("src/main/scala/data/", "data_trimmed_100.csv")
  val builder = new graphBuilder()
  val rg = RaphtoryGraph[String](source, builder)

  val out = "/Users/yolo2519/Desktop/project/outputs"
  rg.pointQuery(PersonalizedPageRank(0.85,0.9,1589940000,1589945000,50,out),1589952842)

//  // FIFO,
//  rg.pointQuery(Taint(1589939554,
//    Set(480137601913333R4833L,
//      7606690438134257376L,
//      3807266728495987608L,
//      -1581127444330411816L),
//    "TIHO",
//    "/tmp"), 1589952842)

//  rg.pointQuery(LinkOnWindow(
//    address = "0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404",
//    startTime = 1589940000,
//    endTime =1589943000,ER
//    fileOutput = "/tmp" ),1589952842)
//

//  rg.pointQuery(FindForkMerge(iter = 50,nodeID = -1L,output =out),1590010475)
//  rg.pointQuery(FindLongChain(iter=5000, path = "/tmp"),1589952842)
  //  rg.pointQuery(GenericTaint(startTime=1589939554, infectedNodes=Set(2172693920751483657L), output = "tmp"), 1589952842)

  //        rg.pointQuery(ConnectedComponents("/tmp"), 1590010475)
  // find in degree
  //        rg.pointQuery(InDegree("/tmp"), 1590010475)
  // find out degree
  //        rg.pointQuery(OutDegree("/tmp"), 1590010475)

  //        rg.pointQuery(FindPattern(), 1589952842)

  // 100
  //        rg.rangeQuery(FindPattern(),start = 1589939000,end = 1589952842,increment = 3600)
  //        rg.rangeQuery(FindPattern(),start = 1589939000,end = 1589952842,increment = 60)
  //20
  //        rg.rangeQuery(FindPattern(),start = 1589939000,end = 1590020000,increment = 7200)
}

//rg.rangeQuery(new ConnectedComponents(Array()), serialiser = nbadew DefaultSerialiser, start=1, end = 32674, increment=1000,windowBatch=List(10000, 1000,100))
//rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,arguments)
//rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,window=100,arguments)
//rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,windowBatch=List(100,50,10),arguments)
//rg.viewQuery(DegreeBasic(), serialiser = new DefaultSerialiser,timestamp = 10000,arguments)
//rg.viewQuery(DegreeBasic(), serialiser = new DefaultSerialiser,timestamp = 10000,window=100,arguments)
