package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.raptor.{RaptorJoin, RaptorJoinResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.awt.Color
import java.awt.image.BufferedImage
import javax.imageio.ImageIO

object RaptorImageExtractor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Raptor Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    try {
      // 1- Load the input data
      val countries: RDD[(Long, IFeature)] = sc.shapefile("NE_countries.zip")
        .zipWithUniqueId()
        .map(f => (f._2, f._1))
      val elevation = sc.geoTiff("HYP_HR")

      // 2- Perform a raptor join between the raster and vector data
      val joinResults: RDD[RaptorJoinResult[Array[Int]]] = RaptorJoin.raptorJoinIDFull(elevation, countries, new BeastOptions())

      // 3- Group the results by country ID to bring together all pixels.
      val countryPixels: RDD[(Long, Iterable[(Int, Int, Array[Int])])] =
        joinResults.map(j => (j.featureID, (j.x, j.y, j.m))).groupByKey()

      // 4- Build a lookup table that maps country ID to its name to use in naming output files
      val countryNames: collection.Map[Long, String] =
        countries.map(c => (c._1, c._2.getAs[String]("NAME"))).collectAsMap()

      // 5- Put the pixels together into an image using the Java image API.
      countryPixels.foreach(x => {
        val pixels = x._2.toArray
        val minX = pixels.map(_._1).min
        val maxX = pixels.map(_._1).max
        val minY = pixels.map(_._2).min
        val maxY = pixels.map(_._2).max
        val image = new BufferedImage(maxX - minX + 1, maxY - minY + 1, BufferedImage.TYPE_INT_ARGB)
        for (pixel <- pixels) {
          val x = pixel._1 - minX
          val y = pixel._2 - minY
          image.setRGB(x, y, new Color(pixel._3(0), pixel._3(1), pixel._3(2)).getRGB)
        }
        // Write the image to the output
        val imagePath = new Path("output-images", countryNames(x._1)+".png")
        val filesystem = imagePath.getFileSystem(new Configuration())
        val out = filesystem.create(imagePath)
        ImageIO.write(image, "png", out)
        out.close()
      })

    } finally {
      spark.stop()
    }
  }
}
