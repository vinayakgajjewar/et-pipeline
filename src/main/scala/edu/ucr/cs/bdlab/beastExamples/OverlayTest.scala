// OverlayTest.scala
// Dummy example to test the RasterOperationsLocal.overlay() function

package edu.ucr.cs.bdlab.beastExamples

// RDPro
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OverlayTest {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val conf = new SparkConf().setAppName("Overlay example")
    conf.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    try {

      // Raster metadata for reshaping
      val metadata = RasterMetadata.create(
        x1 = -50,
        y1 = 40,
        x2 = -60,
        y2 = 30,
        srid = 4326,
        rasterWidth = 10,
        rasterHeight = 10,
        tileWidth = 10,tileHeight = 10
      )

      // Test raster
      val A_pixels = sc.parallelize(Seq(
        (0, 0, 34),
        (1, 4, 47),
        (7, 1, 5),
        (4, 8, 1456),
        (3, 8, 2)
      ))
      val A = sc.rasterizePixels(A_pixels, metadata)

      val B_pixels = sc.parallelize(Seq(
        (0, 0, 5),
        (1, 4, 17),
        (7, 1, 8),
        (4, 8, 1426),
        (3, 8, 69)
      ))
      val B = sc.rasterizePixels(B_pixels, metadata)

      val X_pixels = sc.parallelize(Seq(
        (0, 0, 8),
        (1, 4, 61),
        (7, 1, 17),
        (4, 8, 735),
        (3, 8, 37)
      ))
      val X = sc.rasterizePixels(X_pixels, metadata)

      val C = RasterOperationsLocal.overlay[Int, Int](
        A.asInstanceOf[RDD[ITile[Int]]],
        B.asInstanceOf[RDD[ITile[Int]]],
        X.asInstanceOf[RDD[ITile[Int]]]
      )
      println(C.collect()(0).getPixelValue(1, 4).mkString(" "))
      val D = C.flatten.map(x => x._4(0) + x._4(1) + x._4(2))
      println(D.collect().mkString(" "))
    } finally {
      spark.stop()
    }
  }
}
