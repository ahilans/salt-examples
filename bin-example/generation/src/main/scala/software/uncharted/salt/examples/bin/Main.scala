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

    val lvlFiveZoomTileReq = Seq((5,8,10),
                                (5,8,11), (5,9,11), (5,8,12), (5,9,10))
    val lvlEightZoomTileReq = Seq((8,77,90),
                                  (8,76,90), (8,74,88), (8,75,89), (8,77,89), (8,74,92), (8,75,90), (8,76,92), (8,75,88), (8,74,89), (8,75,91), (8,72,88), (8,75,92),
                                  (8,74,90), (8,78,89), (8,72,89))


    val typicalBinSize: Int = 256

    val wordCloudBinSize: Int = 0


    def valueExtractorHeatmap: (r: Row) => Option[Double] = Some("") match {  //for crossplotTiling ops, you can pull out the heatmap stuff into a new interface and just mix that in can you not (mercator and cartesian will both use them)
      case Some(colName: String) => (r: Row) => {
        val rowIndex = r.schema.fieldIndex(colName)
        if (!r.isNullAt(rowIndex)) Some(r.getDouble(rowIndex)) else None
      }
      case _ => (r: Row) => { Some(1.0) }
    }
    //first split tweet by space.
    def valueExtractorTopics: (r: Row) => Option[Seq[String]] = (r: Row) => {
      val rowIndex = r.schema.fieldIndex("tweet")
      f (!r.isNullAt(rowIndex) && r.getSeq(rowIndex).nonEmpty) Some(r.getSeq(rowIndex)) else None
    }
    //what's the diff between val and def?
    //this probably uses implicit typing. Is it more idiomatic to write out the type?
    val valueExtractorWordCloud = (r: Row) => {
      if (r.isNullAt(termColumnIndex)) {
        None
      } else {
        Some(r.getAs[Seq[String]](termColumnIndex))
      }
    }

    val topicLayerBinAggregator = new TopElementsAggregator[String](3) //3 is the topic limit. Need to figure out what that does.
    val wordCloudLayerBinAggregator = new TopElementsAggregator[String](15) //is there a difference between these two?

    val runTimes: Array[Long]

    //TODO: GET BASIC TILING TO RUN ON DATASET

    //different bin sizes
      //need to add bin size param

    //basic bin example of live tiling. Change parameters based on these.
    val startTime: Long = System.currentTimeMillis
     TileSeqRetriever(inputPath, basicLiveTileReq, basicMercatorProj, typicalBinSize, (r: Row) => Some(1), Some(MinMaxAggregator), CountAggregator)
    val endTime: Long = System.currentTimeMillis
    val runtime: Long = endTime - startTime

    //larger set of tiles
    val startTime2: Long = System.currentTimeMillis
    TileSeqRetriever(inputPath, largerLiveTileReq, basicMercatorProj, typicalBinSize, (r: Row) => Some(1), Some(MinMaxAggregator), CountAggregator)
    val endTime2: Long = System.currentTimeMillis
    val runtime2: Long = endTime - startTime
    //HEATMAP
      //MercatorProjection
      val startTime3: Long = System.currentTimeMillis
      TileSeqRetriever(inputPath, basicLiveTileReq, basicMercatorProj, typicalBinSize, valueExtractorHeatmap, Some(MinMaxAggregator), SumAggregator)
      val endTime3: Long = System.currentTimeMillis
      val runtime3: Long = endTime - startTime

      //CartesianProjection
      val startTime4: Long = System.currentTimeMillis
      TileSeqRetriever(inputPath, basicLiveTileReq, basicCartesianProj, typicalBinSize, valueExtractorHeatmap, Some(MinMaxAggregator), SumAggregator)
      val endTime4: Long = System.currentTimeMillis
      val runtime4: Long = endTime - startTime

    //TOPICS
      //CartesianProjection
      val startTime5: Long = System.currentTimeMillis
      TileSeqRetriever(inputPath, basicLiveTileReq, basicCartesianProj, wordCloudBinSize, valueExtractorTopics, None, topicLayerBinAggregator)
      val endTime5: Long = System.currentTimeMillis
      val runtime5: Long = endTime - startTime

      //MercatorProjcection
      val startTime6: Long = System.currentTimeMillis
      TileSeqRetriever(inputPath, basicLiveTileReq, basicMercatorProj, wordCloudBinSize, valueExtractorTopics, None, topicLayerBinAggregator)
      val endTime6: Long = System.currentTimeMillis
      val runtime6: Long = endTime - startTime

    //WORDCLOUD
      //CartesianProjection
      val startTime7: Long = System.currentTimeMillis
      TileSeqRetriever(inputPath, basicLiveTileReq, basicCartesianProj, wordCloudBinSize, valueExtractorWordCloud, None, wordCloudLayerBinAggregator)
      val endTime7: Long = System.currentTimeMillis
      val runtime7: Long = endTime - startTime

      //MercatorProjcection
      val startTime8: Long = System.currentTimeMillis
      TileSeqRetriever(inputPath, basicLiveTileReq, basicMercatorProj, wordCloudBinSize, valueExtractorWordCloud, None, wordCloudLayerBinAggregator)
      val endTime8: Long = System.currentTimeMillis
      val runtime8: Long = endTime - startTime


      //POTENTIALLY WordCloud
        //top Count + Frequency
        //topic count + frequency

        //KEY: HOW WOULD YOU REPEAT THESE PARAM CHANGES IN ELASTIC SEARCH.
  }
}
