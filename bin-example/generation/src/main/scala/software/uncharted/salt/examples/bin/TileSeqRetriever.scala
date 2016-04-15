package software.uncharted.salt.examples.bin
//import sparkStuff
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, DoubleType}

//import salt stuff
import software.uncharted.salt.core.projection.numeric._
import software.uncharted.salt.core.generation.request._
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.TileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.analytic.numeric._
import software.uncharted.salt.core.analytic._

//import text split
//TODO: this needs to be added to dependency list.
//import software.uncharted.sparkpipe.ops.core.dataframe.text.{split}

import scala.collection.immutable

object TileSeqRetriever {
  //set up default tile size. tile size gives you the number of bins along a dimension.
  val tile_size = 256
  //APPARENTLY METHODS CAN HAVE THEIR OWN IMPLICIT PARAMETERS AS WELL.
  def apply[T,U,V,W,X](
    sc: SparkContext,
    sqlContext: SQLContext,
    datasetPath: String,
    tiles: Seq[(Int, Int, Int)],
    //projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
    projection: String,
    binSize: Int,
    valueExtractor: (Row) => Option[T],
    tileAggregator: Option[Aggregator[V,W,X]],
    binAggregator: Aggregator[T,U,V]): TraversableOnce[((Int, Int, Int))] = {
    //for live tiling:

      //pass in the input data file you will be generating the tiles from
      //pass in the tiles that you want to be generated (will typically be called from the client side based on map zoom level and position)
        //Question: do we have to store the input data into spark memory every time? Can we not cache it? but then what if we cached too many data sets? Spark's in memory size would overflow right?

          // DATASET PATH: uscc0-master0.uncharted.software/user/asuri/taxi_micro.csv
      //with these inputs:

        //load input data into spark
        //generate tiles for those specific tiles
        //return to caller of the tiles.

    //original concept:

      //you need to generate everything first
      //then you can use the tile data from output.

    //however:
      //with live tiling you don't output the generated stuff to another data store.
      //you just return the data. Which means client side should act differently as well.

    //soooo...


    //set up spark definitions(since this is a driver application, it needs the sc and other spark driver stuff I think)

    //can this be pulled out or will every live tiling request have to set up a new instance of sparkContext?


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
    sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter" , "\t")
      .schema(schema)
      .load("hdfs://uscc0-master0.uncharted.software/xdata/data/SummerCamp2015/JulyData-processed/nyc_twitter_merged")//input data assumed to be in hdfs
      .registerTempTable("taxi_micro")

      // filter rows we need
      val input = sqlContext.sql("select lon, lat, userid, tweet from taxi_micro")

      val splitTweetTextDF = split("tweet")(input)
        .rdd.cache()

      //get min and max bounds of data for projection
      val maxBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "max", "lat" -> "max").collect()
      val minBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "min", "lat" -> "min").collect()

      //get projection based on projection type specified
      val projection_object: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = projection match {
        case "mercator" => new MercatorProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13), (minBounds(0).getDouble(1), minBounds(0).getDouble(0)), (maxBounds(0).getDouble(1), maxBounds(0).getDouble(0)))
        case "cartesian" => new CartesianProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13), (minBounds(0).getDouble(1), minBounds(0).getDouble(0)), (maxBounds(0).getDouble(1), maxBounds(0).getDouble(0)))
      }


      //create coord EXTRACTOR
      // Given an input row, return pickup longitude, latitude as a tuple
      val pickupExtractor = (r: Row) => {
        if (r.isNullAt(0) || r.isNullAt(1)) {
          None
        } else {
          Some((r.getDouble(0), r.getDouble(1)))
        }
      }

      //create tile generator to generate tiles
      // Tile Generator object, which houses the generation logic
      val gen = TileGenerator(sc)

      //create series object for generate method.
      val pickups = new Series((tile_size - 1, tile_size - 1),  //(255, 255)
        pickupExtractor, //ROW => (OPTION[(DOUBLE, DOUBLE)])
        projection_object, //PROJECTION
        valueExtractor,          //VALUE EXTRACTOR
        binAggregator,              //BIN AGGREGATOR
        tileAggregator)       //TILE AGGREGATOR

      //create a requestor; use TileSeqRequest to request the sequence of tiles.
      val request = new TileSeqRequest(tiles)

      //call generate method and generate the tiles.
      val rdd = gen.generate(input, pickups, request) //RETURNS RDD[TILE[TC]]

      //once you have an RDD ot tiles, you can transform it to return a tuple.
      //then obtain the changes by collecting the result.
      val output = rdd
        .map(s => pickups(s).get) //returns SeriesData[TC,BC,V,X]
        .map(tile => {
          // Return tuples of tile coordinate, byte array
          (tile.coords)
        })
        .collect() //collects tuples from each executor, combines and returns to driver?

      //now that we have tuples of tile data for every tile, we just return that to the caller

      //prolly would have some middle layer where you would make a request for tiles

      output
  }
}

//Okay, so the goal of this is to check the timing of live tiling requests:
  //what are the parameters for requesting tiles:
    //extractors,
    //projections,
    //aggregators,
    //tileSize


  //what are the parameters involved in requesting live tiles
    //number of tiles
    //tile level?


  //so this function can have methods that vary all of these independently and determine how long it takes

  //what other parameters are there when it comes to live tiling with spark:
    //hardware
      //cpu size
      //in memory cap
      //number of machines

    //spark related parameters:
      //number of nodes
      //executor memory
      //number of executors

    //dataset
      //size of dataset


  //Structure:
    //main function: (runs and times each tile generation)
      //calls: LiveReq methods with different parameters.
