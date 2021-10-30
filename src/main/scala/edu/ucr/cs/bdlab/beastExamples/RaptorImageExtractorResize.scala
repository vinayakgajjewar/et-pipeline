/*
 * Copyright 2021 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.locationtech.jts.geom.Envelope

import java.awt.Color
import java.awt.geom.Point2D
import java.awt.image.BufferedImage
import javax.imageio.ImageIO

/**
 * Joins a raster image with a set of polygons and extracts a separate image for each polygon.
 * Each image is resized to a given fixed size, e.g., 256 x 256. This makes it helpful to use the extracted
 * images in machine learning algorithms which expect input images to be of the same size.
 */
object RaptorImageExtractorResize {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Raptor Image Extractor with Resize")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local")

    val outputResolution: Int = 256
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    try {
      // 1- Load the input data
      val countries: RDD[(Long, IFeature)] = sc.shapefile("NE_countries.zip")
        .zipWithUniqueId()
        .map(f => (f._2, f._1))
      val elevation = sc.geoTiff("HYP_HR_SR")

      // 2- Perform a raptor join between the raster and vector data
      val joinResults: RDD[RaptorJoinResult[Array[Int]]] =
        RaptorJoin.raptorJoinIDFull(elevation, countries, new BeastOptions())

      // 3- Join the results back with country MBRs to rescale the map each pixel to its position in the pixels array
      val countryMBRs: collection.Map[Long, Envelope] = countries.map(x => (x._1, x._2.getGeometry.getEnvelopeInternal))
        .collectAsMap()
      val emptyPixels: Array[Int] = new Array[Int](outputResolution * outputResolution)
      val countryPixels: RDD[(Long, Array[Int])] = joinResults.map(x => (x.featureID, x))
        .aggregateByKey(emptyPixels)((pixels, result) => {
          val mbr = countryMBRs(result.featureID)
          // Map the pixel boundaries to the target image and color all target pixels with the pixel color
          // Notice that some pixels might be partially outside the polygon boundaries because the Raptor join
          // operation finds pixels with a center inside the polygon not the entire pixel inside the polygon
          val pixelLocation = new Point2D.Double()
          result.rasterMetadata.gridToModel(result.x, result.y, pixelLocation)
          val x1 = ((pixelLocation.x - mbr.getMinX) * outputResolution / mbr.getWidth).toInt max 0
          val y1 = (outputResolution - 1 - ((pixelLocation.y - mbr.getMinY) * outputResolution / mbr.getHeight).toInt) max 0
          result.rasterMetadata.gridToModel(result.x + 1.0, result.y + 1.0, pixelLocation)
          val x2 = ((pixelLocation.x - mbr.getMinX) * outputResolution / mbr.getWidth).toInt min (outputResolution - 1)
          val y2 = (outputResolution - 1 - ((pixelLocation.y - mbr.getMinY) * outputResolution / mbr.getHeight).toInt) min (outputResolution - 1)
          val color = new Color(result.m(0), result.m(1), result.m(2)).getRGB
          for (x <- x1 until x2; y <- y1 until y2) {
            val offset = y * outputResolution + x
            pixels(offset) = color
          }
          pixels
        }, (pixels1, pixels2) => {
          for (i <- pixels1.indices; if pixels1(i) == 0)
            pixels1(i) = pixels2(i)
          pixels1
        })

      // 4- Build a lookup table that maps country ID to its name to use in naming output files
      val countryNames: collection.Map[Long, String] =
        countries.map(c => (c._1, c._2.getAs[String]("NAME"))).collectAsMap()

      // 5- Put the pixels together into an image using the Java image API
      countryPixels.foreach(cpixels => {
        val image = new BufferedImage(outputResolution, outputResolution, BufferedImage.TYPE_INT_ARGB)
        for (x <- 0 until outputResolution; y <- 0 until outputResolution) {
          val offset = y * outputResolution + x
          image.setRGB(x, y, cpixels._2(offset))
        }
        // Write the image to the output
        val imagePath = new Path("output-images", countryNames(cpixels._1)+".png")
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