package software.uncharted.salt.examples.bin
//import sparkStuff
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import redis.clients.jedis.Jedis



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


//S3 Client Stuff
import java.io.{ByteArrayOutputStream, ByteArrayInputStream, InputStream}
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{PutObjectRequest, CannedAccessControlList, ObjectMetadata}

//need to add aws to gradle




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

//ask mike for the cpus size of each node (ES)
//count()
//S3
//get cluster size for ES to create equivalent set of workers executors
//ES cannot scale up. Once you have an index with set config that's what you get
//Spark can scale up. Speed Incerase
//ES handles requests Serially? Only request tiles one at a itme
//Spark can request multiple tiles

//ES: 8 machines,  8 cores, 64 G ram

//Kevin has code that generates tiles


object TileSeqRetriever {
  def apply(sc: SparkContext, sqlContext: SQLContext, input: DataFrame) = {
    //load input into spark
    // val maxBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "max", "lat" -> "max").collect()
    // val minBounds = sqlContext.sql("select lon, lat from taxi_micro").agg("lon" -> "min", "lat" -> "min").collect()
    val bounds = input.selectExpr("max(lon)", "min(lon)", "max(lat)", "min(lat)").collect()(0)
    new TileSeqRetriever(sc, sqlContext, input.rdd, bounds)
  }
}

class TileSeqRetriever(sc: SparkContext, sqlContext: SQLContext, input: RDD[Row], bounds: Row) {
  //set up default tile size. tile size gives you the number of bins along a dimension.
  val tile_size = 256
  //APPARENTLY METHODS CAN HAVE THEIR OWN IMPLICIT PARAMETERS AS WELL.
  def requestTile[T,U,V,W,X](
    tiles: Seq[(Int, Int, Int)],
    //projection: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)],
    projection: String,
    binSize: Int,
    valueExtractor: (Row) => Option[T],
    tileAggregator: Option[Aggregator[Double,W,(Double, Double)]],
    binAggregator: Aggregator[T,U,Double]): RDD[((Int, Int, Int), Seq[Byte])] = {


      //get min and max bounds of data for projection


      //get projection based on projection type specified
      val projection_object: NumericProjection[(Double, Double), (Int, Int, Int), (Int, Int)] = projection match {
        case "mercator" => new MercatorProjection(Seq(0,12,13))
        case "cartesian" => new CartesianProjection(Seq(0,12,13), (bounds.getDouble(3), bounds.getDouble(2)), (bounds.getDouble(1), bounds.getDouble(0)))
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

      val rdd = gen.generate(input, pickups, request).flatMap(s => pickups(s)) //returns SeriesData[TC,BC,V,X]
      val output = rdd
      .map({ tile =>
          val data = for (bin <- tile.bins; i <- 0 until 8) yield {
            val data = java.lang.Double.doubleToLongBits(bin)
            ((data >> (i * 8)) & 0xff).asInstanceOf[Byte]
          }
          (tile.coords, data.toSeq) //because tile.bins is a TraversableOnce, so convert to Seq to match rest of code (currently uses Seq)
        })

        output.foreachPartition { tileDataIter =>
          val jedis = new Jedis("10.64.8.166", 6379)
            tileDataIter.foreach { tileData =>
            val coord = tileData._1
            // store tile in bucket as layerName/level-xIdx-yIdx.bin
            val key = s"testData/${coord._1}/${coord._2}/${coord._3}"

            val result = jedis.set(key, "hi")
            println("SAFDSAFDSADFASDFASDFASDFASDFASDFSADFASDFASDFSADFAS")
          }
        }

      // S3 output veresion
      // val output = rdd
      //   .filter(t => t.bins.density() > 0)
      //   .map({ tile =>
      //     val data = for (bin <- tile.bins; i <- 0 until 8) yield {
      //       val data = java.lang.Double.doubleToLongBits(bin)
      //       ((data >> (i * 8)) & 0xff).asInstanceOf[Byte]
      //     }
      //     (tile.coords, data.toSeq) //because tile.bins is a TraversableOnce, so convert to Seq to match rest of code (currently uses Seq)
      //   })
      //
      //   output.foreachPartition { tileDataIter =>
      //     val s3Client = new AmazonS3Client(new BasicAWSCredentials("AKIAIRJE3R57Z2PRC5CA", "vbcT/1yDD10YofItNnMcOIQprGPgyGYJNuW02uYM"))
      //     val jedis = new Jedis("localhost")
      //       tileDataIter { tileData =>
      //       val coord = tileData._1
      //       // store tile in bucket as layerName/level-xIdx-yIdx.bin
      //       val key = s"testData/${coord._1}/${coord._2}/${coord._3}.bin"
      //
      //       val is = new ByteArrayInputStream(tileData._2.toArray)
      //       val meta = new ObjectMetadata()
      //
      //       meta.setContentType("application/octet-stream")
      //       s3Client.putObject(new PutObjectRequest("spark-live-tile-benchmark-test", key, is, meta) // scalastyle:ignore
      //       .withCannedAcl(CannedAccessControlList.PublicRead))
      //     }
      //   }

      //import xdata-pipeline-ops jar and import S3 client LATER.



      output
  }
}
