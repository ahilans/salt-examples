package software.uncharted.salt.examples.bin

object Main {
  def apply(args: Array[String]) = {
    if (args.length > 1) {
      //quit
      println("Too many arguments")
      System.exit(-1)
    }

    val inputPath = args(0)

    val basicLiveTileReq = Seq((1,0,0), (1,1,0), (1,0,1), (1,1,1))
    val basicMercatorProj = new MercatorProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13))
    val basicCartesianProj = new CartesianProjection(Seq(0,1,2,3,4,5,6,7,8,9,10,11,12,13))

    val largerLiveTileReq = Seq((3,0,0), (3,0,1), (3,0,2), (3,0,3), (3,0,4), (3,1,0), (3,2,0), (3,3,0), (3,4,0), (3,1,1), (3,1,2),
     (3,1,3), (3,1,4), (3,2,1), (3,2,2), (3,2,3), (3,2,4))//8*8 tiles

    val runTimes: Array[Long]

    //basic bin example of live tiling. Change parameters based on these.
    val startTime: Long = System.currentTimeMillis
     TileSeqRetriever(inputPath, basicLiveTileReq, basicMercatorProj, (r: Row) => Some(1), Some(MinMaxAggregator), CountAggregator)
    val endTime: Long = System.currentTimeMillis

    val runtime: Long = endTime - startTime
    //larger set of tiles
    TileSeqRetriever(inputPath, largerLiveTileReq, basicMercatorProj, (r: Row) => Some(1), Some(MinMaxAggregator), CountAggregator)

    //heatMap Live Tiling
    //MercatorProjection
    TileSeqRetriever(inputPath, basicLiveTileReq, basicMercatorProj, (r: Row) => Some(1) ,Some(MinMaxAggregator), SumAggregator)

    //CartesianProjection
    TileSeqRetriever(inputPath, basicLiveTileReq, basicCartesianProj, (r: Row) => Some(1) ,Some(MinMaxAggregator), SumAggregator)
    //different bin sizes
      //need to add bin size param


      //POTENTIALLY WordCloud
        //top Count + Frequency
        //topic count + frequency

        //KEY: HOW WOULD YOU REPEAT THESE PARAM CHANGES IN ELASTIC SEARCH.
  }
}
