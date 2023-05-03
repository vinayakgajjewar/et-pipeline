package edu.ucr.cs.bdlab.beastExamples

// RDPro
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.raptor.RasterOperationsFocal
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GeoTIFFExample {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val conf = new SparkConf().setAppName("Evapotranspiration Pipeline")
    conf.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    try {

      val metadata = RasterMetadata.create(x1 = -50, y1 = 40, x2 = -60, y2 = 30, srid = 4326, rasterWidth = 10, rasterHeight = 10, tileWidth = 10, tileHeight = 10)
      var T = sc.geoTiff[Array[Float]]("file:///Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/data/air.sfc.2023.tif")
      T = RasterOperationsFocal.reshapeNN(T, metadata)
      val T_flat = T.flatten
      T_flat.map(_._4(0) - 272.15f)
    }
  }
}
