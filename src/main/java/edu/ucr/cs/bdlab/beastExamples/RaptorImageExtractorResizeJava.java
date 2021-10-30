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
package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.raptor.RaptorJoin;
import edu.ucr.cs.bdlab.raptor.RaptorJoinResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.OutputStream;
import java.util.Map;

public class RaptorImageExtractorResizeJava {
  public static void main(String[] args) {
    // Initialize Spark
    SparkConf conf = new SparkConf().setAppName("Beast Example");

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local");

    // Create Spark session (for Dataframe API) and Spark context (for RDD API)
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    JavaSpatialSparkContext sparkContext = new JavaSpatialSparkContext(sparkSession.sparkContext());

    final int outputResolution = 256;

    try {
      // 1- Load the input data
      JavaPairRDD<Long, IFeature> countries = sparkContext.shapefile("NE_countries.zip")
          .zipWithUniqueId()
          .mapToPair(f -> new Tuple2<>(f._2, f._1));
      JavaRDD<ITile> elevation = sparkContext.geoTiff("HYP_HR_SR");

      // 2- Perform a raptor join between the raster and vector data
      JavaRDD<RaptorJoinResult<int[]>> joinResults =
          RaptorJoin.raptorJoinIDFullJ(elevation, countries, new BeastOptions());

      // 3- Join the results back with country MBRs to rescale the map each pixel to its position in the pixels array
      Map<Long, Envelope> countryMBRs = countries.mapToPair(c -> new Tuple2<>(c._1, c._2.getGeometry().getEnvelopeInternal()))
          .collectAsMap();
      int[] emptyPixels = new int[outputResolution * outputResolution];
      JavaPairRDD<Long, int[]> countryPixels = joinResults.mapToPair(x -> new Tuple2<>(x.featureID(), x))
          .aggregateByKey(emptyPixels, (pixels, result) -> {
            Envelope mbr = countryMBRs.get(result.featureID());
            // Map the pixel boundaries to the target image and color all target pixels with the pixel color
            // Notice that some pixels might be partially outside the polygon boundaries because the Raptor join
            // operation finds pixels with a center inside the polygon not the entire pixel inside the polygon
            Point2D.Double pixelLocation = new Point2D.Double();
            result.rasterMetadata().gridToModel(result.x(), result.y(), pixelLocation);
            int x1 = Math.max(0, (int)(((pixelLocation.x - mbr.getMinX()) * outputResolution / mbr.getWidth())));
            int y1 = Math.max(0, (outputResolution - 1 - (int) (((pixelLocation.y - mbr.getMinY())) * outputResolution / mbr.getHeight())));
            result.rasterMetadata().gridToModel(result.x() + 1.0, result.y() + 1.0, pixelLocation);
            int x2 = Math.min(outputResolution - 1, (int)((pixelLocation.x - mbr.getMinX()) * outputResolution / mbr.getWidth()));
            int y2 = Math.min(outputResolution - 1, outputResolution - 1 - (int)(((pixelLocation.y - mbr.getMinY()) * outputResolution / mbr.getHeight())));
            int color = new Color(result.m()[0], result.m()[1], result.m()[2]).getRGB();
            for (int x = x1; x < x2; x++) {
              for (int y = y1; y < y2; y++) {
                int offset = y * outputResolution + x;
                pixels[offset] = color;
              }
            }
            return pixels;
          }, (pixels1, pixels2) -> {
            for (int i = 0; i < pixels1.length; i++) {
              if (pixels1[i] == 0)
                pixels1[i] = pixels2[i];
            }
            return pixels1;
          });
      // 4- Build a lookup table that maps country ID to its name to use in naming output files
      Map<Long, String> countryNames =
          countries.mapToPair(c -> new Tuple2<>(c._1, c._2.getAs("NAME").toString())).collectAsMap();

      // 5- Put the pixels together into an image using the Java image API.
      countryPixels.foreach(cpixels -> {
        BufferedImage image = new BufferedImage(outputResolution, outputResolution, BufferedImage.TYPE_INT_ARGB);
        for (int x = 0; x < outputResolution; x++) {
          for (int y = 0; y < outputResolution; y++) {
            int offset = y * outputResolution + x;
            image.setRGB(x, y, cpixels._2[offset]);
          }
        }
        // Write the image to the output
        Path imagePath = new Path("output-images", countryNames.get(cpixels._1)+".png");
        FileSystem filesystem = imagePath.getFileSystem(new Configuration());
        OutputStream out = filesystem.create(imagePath);
        ImageIO.write(image, "png", out);
        out.close();
      });

    } finally {
      // Clean up Spark session
      sparkSession.stop();
    }
  }
}
