package software.uncharted.salt.examples.bin
//import salt stuff
import software.uncharted.salt.core.projection.numeric._
import software.uncharted.salt.core.generation.request._
import software.uncharted.salt.core.analytic.numeric._
import software.uncharted.salt.core.analytic.collection._

import scala.collection.immutable
import scala.util.Random
//import spark stuff
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, DoubleType}
import redis.clients.jedis.Jedis
import scala.math._

object Main {

  private case class Bounds(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double)

  private def genTileSequence(numTiles: Int, level: Int, bounds: Bounds) = {
    def rand(min: Double, max: Double) = min + (Random.nextDouble() * (max - min))
    def randIndex = {
      val lon = rand(bounds.minLon, bounds.maxLon)
      val lat = rand(bounds.minLat, bounds.maxLat)
      val tileX = ((lon + 180.0) / 360.0 * (1 << level)).toInt
      val tileY = (1 << level) - ((1 - log(tan(toRadians(lat)) + 1 / cos(toRadians(lat))) / Pi) / 2.0 * (1 << level)).toInt
      (level, tileX, tileY)
    }
    for (i <- 0 until numTiles) yield randIndex
  }
  private val nycBounds = Bounds(-74.06, 40.54, -72.72, 40.92)
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



    val initStartTimeA: Long = System.currentTimeMillis
    val indexedCols: immutable.IndexedSeq[StructField] = immutable.IndexedSeq(
    StructField("date", StringType),
    StructField("userid", DoubleType),
    StructField("username", StringType),
    StructField("tweetid", StringType),
    StructField("tweet", StringType),
    StructField("hashtags", StringType),

    StructField("lon", DoubleType),
    StructField("lat", DoubleType),

    StructField("country", StringType),
    StructField("state", StringType),
    StructField("city", StringType),
    StructField("language", StringType))

    val schema = StructType(indexedCols)

    //upload input data file into spark.
    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter" , "\t")
      .schema(schema)
      .load(s"hdfs://$inputPath")//input data assumed to be in hdfs //"hdfs://uscc0-master0.uncharted.software/xdata/data/SummerCamp2015/JulyData-processed/nyc_twitter_merged"
      .select("lon", "lat")
      .repartition(200)
      .cache

    //create instance of tileRetriever for that specific data set
    val nycTwitterDataTileRetriever = TileSeqRetriever(sc, sqlContext, df)
    val initEndTimeA: Long = System.currentTimeMillis
    val initRuntimeA: Long = initEndTimeA - initStartTimeA

    //val partialFunc = TileRetriever2(sc, sqlContext, input)_

    //test params
    val mercator = "mercator"
    val cartesian = "cartesian"
    val typicalBinSize: Int = 256

    //run same tile req multiple tmie to see average time to pull from spark
      //tihs is because after many calls caching is done possibly?


    val startTimeE: Long = System.currentTimeMillis
    nycTwitterDataTileRetriever.requestTile(Seq((12,1206,1542)), mercator, 256, ((r: Row) => Some(1)), Some(MinMaxAggregator), CountAggregator)
    val endTimeE: Long = System.currentTimeMillis
    val runtimeE: Long = endTimeE - startTimeE

    val tileSequences = for (i <- 0 until 4) yield genTileSequence(1024, 15, nycBounds)
    val sequenceTimings = tileSequences.map { sequence =>
      val startTime = System.currentTimeMillis
      nycTwitterDataTileRetriever.requestTile(sequence, mercator, 256, r => Some(1), Some(MinMaxAggregator), CountAggregator)
      System.currentTimeMillis - startTime
    }


    println("=====================================================================")
    println("Time to load data into spark. This is the version where it's supposed to upload to s3")
    println(initRuntimeA)
    println("=====================================================================")
    println("Time to run job first time")
    println(runtimeE)

    println("=====================================================================")
    println("Times for runnning SEQUENCE OF TILES")
    println("=====================================================================")
    sequenceTimings.foreach(println(_))

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
