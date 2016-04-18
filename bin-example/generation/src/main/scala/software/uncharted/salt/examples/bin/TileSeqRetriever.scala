package software.uncharted.salt.examples.bin
//import sparkStuff
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import org.apache.spark.rdd.RDD



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

// object TileRetriever2 {
//   def apply[T,U,V,W,X](sc: SparkContext, sqlContext: SQLContext, input: RDD[Row])(tiles: Seq[(Int, Int, Int)],
//   projection: String,
//   binSize: Int,
//   valueExtractor: (Row) => Option[T],
//   tileAggregator: Option[Aggregator[V,W,X]],
//   binAggregator: Aggregator[T,U,V]): RDD[(Int, Int, Int)] = {
//     val tile_size = 256
//     val maxBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "max", "lat" -> "max").collect()
//     val minBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "min", "lat" -> "min").collect()
//
//     //get projection based on projection type specified
//     val projection_object: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = projection match {
//       case "mercator" => new MercatorProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13), (minBounds(0).getDouble(1), minBounds(0).getDouble(0)), (maxBounds(0).getDouble(1), maxBounds(0).getDouble(0)))
//       case "cartesian" => new CartesianProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13), (minBounds(0).getDouble(1), minBounds(0).getDouble(0)), (maxBounds(0).getDouble(1), maxBounds(0).getDouble(0)))
//     }
//
//
//     //create coord EXTRACTOR
//     // Given an input row, return pickup longitude, latitude as a tuple
//     val pickupExtractor = (r: Row) => {
//       if (r.isNullAt(0) || r.isNullAt(1)) {
//         None
//       } else {
//         Some((r.getDouble(0), r.getDouble(1)))
//       }
//     }
//
//     //create tile generator to generate tiles
//     // Tile Generator object, which houses the generation logic
//     val gen = TileGenerator(sc)
//
//     //create series object for generate method.
//     val pickups = new Series((tile_size - 1, tile_size - 1),  //(255, 255)
//       pickupExtractor, //ROW => (OPTION[(DOUBLE, DOUBLE)])
//       projection_object, //PROJECTION
//       valueExtractor,          //VALUE EXTRACTOR
//       binAggregator,              //BIN AGGREGATOR
//       tileAggregator)       //TILE AGGREGATOR
//
//     //create a requestor; use TileSeqRequest to request the sequence of tiles.
//     val request = new TileSeqRequest(tiles)
//
//     //call generate method and generate the tiles.
//     val rdd = gen.generate(input, pickups, request) //RETURNS RDD[TILE[TC]]
//
//     //once you have an RDD ot tiles, you can transform it to return a tuple.
//     //then obtain the changes by collecting the result.
//     val output = rdd
//       .map(s => pickups(s).get) //returns SeriesData[TC,BC,V,X]
//       .map(tile => {
//         // Return tuples of tile coordinate, byte array
//         (tile.coords)
//       })
//     output
//   }
// }

object TileSeqRetriever {
  def apply(sc: SparkContext, sqlContext: SQLContext, input: RDD[Row]) = {
    //load input into spark
    val maxBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "max", "lat" -> "max").collect()
    val minBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "min", "lat" -> "min").collect()
    new TileSeqRetriever(sc, sqlContext, input, maxBounds, minBounds)
  }
}

class TileSeqRetriever(sc: SparkContext, sqlContext: SQLContext, input: RDD[Row], minBounds: Array[Row], maxBounds: Array[Row]) {
  //set up default tile size. tile size gives you the number of bins along a dimension.
  val tile_size = 256
  //APPARENTLY METHODS CAN HAVE THEIR OWN IMPLICIT PARAMETERS AS WELL.
  def requestTile[T,U,V,W,X](
    tiles: Seq[(Int, Int, Int)],
    //projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
    projection: String,
    binSize: Int,
    valueExtractor: (Row) => Option[T],
    tileAggregator: Option[Aggregator[V,W,X]],
    binAggregator: Aggregator[T,U,V]): RDD[(Int, Int, Int)] = {


      //get min and max bounds of data for projection


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

      val gen = TileGenerator(sc)

      //create series object for generate method.
      val pickups = new Series((tile_size - 1, tile_size - 1),  //(255, 255)
        pickupExtractor, //ROW => (OPTION[(DOUBLE, DOUBLE)])
        projection_object, //PROJECTION
        valueExtractor,          //VALUE EXTRACTOR
        binAggregator,              //BIN AGGREGATOR
        tileAggregator)       //TILE AGGREGATOR

      val request = new TileSeqRequest(tiles)

      val rdd = gen.generate(input, pickups, request) //RETURNS RDD[TILE[TC]]

      val output = rdd
        .map(s => pickups(s).get) //returns SeriesData[TC,BC,V,X]
        .map(tile => {

          (tile.coords)
        })

      output
  }
}
