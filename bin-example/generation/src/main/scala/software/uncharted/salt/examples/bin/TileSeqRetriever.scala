package software.uncharted.salt.examples.bin
//import sparkStuff
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

//import salt stuff
import software.uncharted.salt.core.projection.numeric._
import software.uncharted.salt.core.generation.request._
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.TileGenerator
import software.uncharted.salt.core.generation.output.SeriesData
import software.uncharted.salt.core.analytic.numeric._

object TileSeqRetriever {
  def createByteBuffer(tile: SeriesData[(Int, Int, Int), (Int, Int), Double, (Double, Double)]): Array[Byte] = {
    val byteArray = new Array[Byte](tileSize * tileSize * 8)
    var j = 0
    tile.bins.foreach(b => {
      val data = java.lang.Double.doubleToLongBits(b)
      for (i <- 0 to 7) {
        byteArray(j) = ((data >> (i * 8)) & 0xff).asInstanceOf[Byte]
        j += 1
      }
    })
    byteArray
  }
  //APPARENTLY METHODS CAN HAVE THEIR OWN IMPLICIT PARAMETERS AS WELL.
  def apply[TC, T](datasetPath: String, tiles: Seq[TC], projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)], valueExtractor: ((r: Row) => Option[T]), tileAggregator: Some(Aggregator), binAggregator: Aggregator): TraversableOnce(TC, Array[Byte]) = {
    //for live tiling:

      //pass in the input data file you will be generating the tiles from
      //pass in the tiles that you want to be generated (will typically be called from the client side based on map zoom level and position)
        //Question: do we have to store the input data into spark memory every time? Can we not cache it? but then what if we cached too many data sets? Spark's in memory size would overflow right?


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

    //set up default tile size. tile size gives you the number of bins along a dimension.
    val tile_size = 256

    //set up spark definitions(since this is a driver application, it needs the sc and other spark driver stuff I think)

    //can this be pulled out or will every live tiling request have to set up a new instance of sparkContext?
    val conf = new SparkConf().setAppName("salt-bin-example")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //upload input data file into spark.
    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"hdfs://$datasetPath")//input data assumed to be in hdfs
      .registerTempTable("taxi_micro")

      // filter rows we need
      val input = sqlContext.sql("select pickup_lon, pickup_lat from taxi_micro")
        .rdd.cache()

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

      //projection needs sequence of levels for projection
      val zoomLevels = Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13)

      //create series object for generate method.
      val pickups = new Series((tileSize - 1, tileSize - 1),  //(255, 255)
        pickupExtractor, //ROW => (OPTION[(DOUBLE, DOUBLE)])
        projection //PROJECTION
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
          (tile.coords, createByteBuffer(tile))
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
