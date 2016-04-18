package software.uncharted.salt.examples.bin
//import salt stuff
import software.uncharted.salt.core.projection.numeric._
import software.uncharted.salt.core.generation.request._
import software.uncharted.salt.core.analytic.numeric._
import software.uncharted.salt.core.analytic.collection._

//import spark stuff
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object Main {
  def main(args: Array[String]) = {
    if (args.length > 1) {
      //quit
      println("Too many arguments")
      System.exit(-1)
    }

    //try on single tiles, run multiple times on a single tile because the first few times on spark doesn't give you the long term timing capability?

    //try to cache the data of jobs in a specific place.

    //so:
      //run different tiling layers..
        //for each tiling layer, request a set of tiles one by one. (to match with ES)
          //run each tile request multiple times in a row and see the result. see if the time changes
        //Also note that Spark can request tiles in sequence (may be an advantage to ES)


    //equivalent script uisng prism: request one tile coordinate using prism's library. NYC Twitter Data is already ingested into ES Cluster.
      //I think prism uses redis?? probably not. Prism is a library. ES cluster probably does though. Need to double check.

    //then try running with sets of data.

    //pulled out to the main because a spark applciation cannot have multiple contexts.
    val conf = new SparkConf().setAppName("salt-bin-example")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inputPath = args(0)

    val singleTileReq = Seq((0,0,0))

    val mercator = "mercator"
    val cartesian = "cartesian"
    val typicalBinSize: Int = 256

    //run same tile req multiple tmie to see average time to pull from spark
      //tihs is because after many calls caching is done possibly?

    val startTimeA: Long = System.currentTimeMillis
    TileSeqRetriever(sc, sqlContext, inputPath, singleTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeA: Long = System.currentTimeMillis
    val runtimeA: Long = endTimeA - startTimeA

    val startTimeB: Long = System.currentTimeMillis
    TileSeqRetriever(sc, sqlContext, inputPath, singleTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeB: Long = System.currentTimeMillis
    val runtimeB: Long = endTimeB - startTimeB

    val startTimeC: Long = System.currentTimeMillis
    TileSeqRetriever(sc, sqlContext, inputPath, singleTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeC: Long = System.currentTimeMillis
    val runtimeC: Long = endTimeC - startTimeC

    val startTimeD: Long = System.currentTimeMillis
    TileSeqRetriever(sc, sqlContext, inputPath, singleTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeD: Long = System.currentTimeMillis
    val runtimeD: Long = endTimeD - startTimeD

    val startTimeE: Long = System.currentTimeMillis
    TileSeqRetriever(sc, sqlContext, inputPath, singleTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeE: Long = System.currentTimeMillis
    val runtimeE: Long = endTimeE - startTimeE

    val startTimeF: Long = System.currentTimeMillis
    TileSeqRetriever(sc, sqlContext, inputPath, singleTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeF: Long = System.currentTimeMillis
    val runtimeF: Long = endTimeF - startTimeF

    println(runtimeA)
    println(runtimeB)
    println(runtimeC)
    println(runtimeD)
    println(runtimeE)
    println(runtimeF)



    // //run it for different tiles, still requesting one at a time though
    //   //repeat processing same tiles a couple times to see if run time decreases.
    //   for (i <- 1 to 5) {
    //
    //     val startTimeG: Long = System.currentTimeMillis
    //     TileSeqRetriever(sc, sqlContext, inputPath, Seq((1,0,0)), mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    //     val endTimeG: Long = System.currentTimeMillis
    //     val runtimeG: Long = endTimeG - startTimeG
    //
    //     val startTimeH: Long = System.currentTimeMillis
    //     TileSeqRetriever(sc, sqlContext, inputPath, Seq((1,0,1)), mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    //     val endTimeH: Long = System.currentTimeMillis
    //     val runtimeH: Long = endTimeH - startTimeH
    //
    //     val startTimeG: Long = System.currentTimeMillis
    //     TileSeqRetriever(sc, sqlContext, inputPath, Seq((1,1,0)), mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    //     val endTimeG: Long = System.currentTimeMillis
    //     val runtimeG: Long = endTimeG - startTimeG
    //
    //     val startTimeH: Long = System.currentTimeMillis
    //     TileSeqRetriever(sc, sqlContext, inputPath, Seq((1,1,1)), mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    //     val endTimeH: Long = System.currentTimeMillis
    //     val runtimeH: Long = endTimeH - startTimeH
    //
    //   }
    //
    // //you can also try it for different tiling jobs
    // val valueExtractorHeatmap = Some("userid") match {  //for crossplotTiling ops, you can pull out the heatmap stuff into a new interface and just mix that in can you not (mercator and cartesian will both use them)
    //   case Some(colName: String) => (r: Row) => {
    //     val rowIndex = r.schema.fieldIndex(colName)
    //     if (!r.isNullAt(rowIndex)) Some(r.getDouble(rowIndex)) else None
    //   }
    //   case _ => (r: Row) => { Some(1.0) }
    // }
    //
    // val valueExtractorTopics: (Row) => Option[Seq[String]] = (r: Row) => {
    //   val rowIndex = r.schema.fieldIndex("tweet")
    //   if (!r.isNullAt(rowIndex) && r.getSeq(rowIndex).nonEmpty) Some(r.getSeq(rowIndex)) else None
    // }
    //
    // val valueExtractorWordCloud = (r: Row) => {
    //   if (r.isNullAt(termColumnIndex)) {
    //     None
    //   } else {
    //     Some(r.getAs[Seq[String]](termColumnIndex))
    //   }
    // }
    //
    // val startTimeG: Long = System.currentTimeMillis
    // TileSeqRetriever(sc, sqlContext, inputPath, Seq((1,0,0)), mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    // val endTimeG: Long = System.currentTimeMillis
    // val runtimeG: Long = endTimeG - startTimeG




    // val basicLiveTileReq = Seq((1,0,0), (1,1,0), (1,0,1), (1,1,1))
    // //val basicMercatorProj = new MercatorProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13))
    // //val basicCartesianProj = new CartesianProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13))
    //
    // val largerLiveTileReq = Seq((3,0,0), (3,0,1), (3,0,2), (3,0,3), (3,0,4), (3,1,0), (3,2,0), (3,3,0), (3,4,0), (3,1,1), (3,1,2),
    //  (3,1,3), (3,1,4), (3,2,1), (3,2,2), (3,2,3), (3,2,4))//8*8 tiles
    //
    // val lvlFiveZoomTileReq = Seq((5,8,10),
    //                             (5,8,11), (5,9,11), (5,8,12), (5,9,10))
    // val lvlEightZoomTileReq = Seq((8,77,90),
    //                               (8,76,90), (8,74,88), (8,75,89), (8,77,89), (8,74,92), (8,75,90), (8,76,92), (8,75,88), (8,74,89), (8,75,91), (8,72,88), (8,75,92),
    //                               (8,74,90), (8,78,89), (8,72,89))
    //
    // val wordCloudBinSize: Int = 0
    //
    //
    // val valueExtractorHeatmap = Some("userid") match {  //for crossplotTiling ops, you can pull out the heatmap stuff into a new interface and just mix that in can you not (mercator and cartesian will both use them)
    //   case Some(colName: String) => (r: Row) => {
    //     val rowIndex = r.schema.fieldIndex(colName)
    //     if (!r.isNullAt(rowIndex)) Some(r.getDouble(rowIndex)) else None
    //   }
    //   case _ => (r: Row) => { Some(1.0) }
    // }
    // //first split tweet by space.
    // val valueExtractorTopics: (Row) => Option[Seq[String]] = (r: Row) => {
    //   val rowIndex = r.schema.fieldIndex("tweet")
    //   if (!r.isNullAt(rowIndex) && r.getSeq(rowIndex).nonEmpty) Some(r.getSeq(rowIndex)) else None
    // }
    // //what's the diff between val and def?
    // //this probably uses implicit typing. Is it more idiomatic to write out the type?
    // // val valueExtractorWordCloud = (r: Row) => {
    // //   if (r.isNullAt(termColumnIndex)) {
    // //     None
    // //   } else {
    // //     Some(r.getAs[Seq[String]](termColumnIndex))
    // //   }
    // // }
    //
    // val topicLayerBinAggregator = new TopElementsAggregator[String](3) //3 is the topic limit. Need to figure out what that does.
    // val wordCloudLayerBinAggregator = new TopElementsAggregator[String](15) //is there a difference between these two?
    //
    // //TODO: GET BASIC TILING TO RUN ON DATASET
    //
    // //different bin sizes
    //   //need to add bin size param
    //
    // //basic bin example of live tiling. Change parameters based on these.
    // val startTime: Long = System.currentTimeMillis
    // TileSeqRetriever(sc, sqlContext, inputPath, lvlEightZoomTileReq, mercator, typicalBinSize, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    // val endTime: Long = System.currentTimeMillis
    // val runtime: Long = endTime - startTime
    //
    // //larger set of tiles
    // val startTime2: Long = System.currentTimeMillis
    // TileSeqRetriever(sc, sqlContext, inputPath, lvlEightZoomTileReq, mercator, typicalBinSize, (r: Row) => Some(1), Some(MinMaxAggregator), CountAggregator)
    // val endTime2: Long = System.currentTimeMillis
    // val runtime2: Long = endTime2 - startTime2
    // //HEATMAP
    //   //MercatorProjection
    //   val startTime3: Long = System.currentTimeMillis
    //   TileSeqRetriever(sc, sqlContext, inputPath, lvlEightZoomTileReq, mercator, typicalBinSize, valueExtractorHeatmap, Some(MinMaxAggregator), SumAggregator)
    //   val endTime3: Long = System.currentTimeMillis
    //   val runtime3: Long = endTime3 - startTime3
    // //
    // //   //CartesianProjection
    // //   val startTime4: Long = System.currentTimeMillis
    // //   TileSeqRetriever(inputPath, lvlEightZoomTileReq, basicCartesianProj, typicalBinSize, valueExtractorHeatmap, Some(MinMaxAggregator), SumAggregator)
    // //   val endTime4: Long = System.currentTimeMillis
    // //   val runtime4: Long = endTime - startTime
    // //
    // // //TOPICS
    // //   //CartesianProjection
    // //   val startTime5: Long = System.currentTimeMillis
    // //   TileSeqRetriever(inputPath, lvlEightZoomTileReq, basicCartesianProj, wordCloudBinSize, valueExtractorTopics, None, topicLayerBinAggregator)
    // //   val endTime5: Long = System.currentTimeMillis
    // //   val runtime5: Long = endTime - startTime
    // //
    //   //MercatorProjcection
    //   // val startTime6: Long = System.currentTimeMillis
    //   // TileSeqRetriever(inputPath, lvlEightZoomTileReq, basicMercatorProj, wordCloudBinSize, valueExtractorTopics, None, topicLayerBinAggregator)
    //   // val endTime6: Long = System.currentTimeMillis
    //   // val runtime6: Long = endTime6 - startTime6
    // //
    // // //WORDCLOUD
    // //   //CartesianProjection
    // //   val startTime7: Long = System.currentTimeMillis
    // //   TileSeqRetriever(inputPath, lvlEightZoomTileReq, basicCartesianProj, wordCloudBinSize, valueExtractorWordCloud, None, wordCloudLayerBinAggregator)
    // //   val endTime7: Long = System.currentTimeMillis
    // //   val runtime7: Long = endTime - startTime
    // //
    // //   //MercatorProjcection
    // //   val startTime8: Long = System.currentTimeMillis
    // //   TileSeqRetriever(inputPath, lvlEightZoomTileReq, basicMercatorProj, wordCloudBinSize, valueExtractorWordCloud, None, wordCloudLayerBinAggregator)
    // //   val endTime8: Long = System.currentTimeMillis
    // //   val runtime8: Long = endTime - startTime
    //
    //   println(runtime)
    //   println(runtime2)
    //   println(runtime3)
    //   // println(runtime4)
    //   // println(runtime5)
    //   // println(runtime6)
    //   // println(runtime7)
    //   // println(runtime8)
  }
}
