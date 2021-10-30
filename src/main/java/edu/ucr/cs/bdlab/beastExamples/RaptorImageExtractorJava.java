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
import scala.Tuple2;
import scala.Tuple3;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RaptorImageExtractorJava {
  public static void main(String[] args) {
    // Initialize Spark
    SparkConf conf = new SparkConf().setAppName("Beast Example");

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local");

    // Create Spark session (for Dataframe API) and Spark context (for RDD API)
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    JavaSpatialSparkContext sparkContext = new JavaSpatialSparkContext(sparkSession.sparkContext());

    try {
      // 1- Load the input data
      JavaPairRDD<Long, IFeature> countries = sparkContext.shapefile("NE_countries.zip")
          .zipWithUniqueId()
          .mapToPair(f -> new Tuple2<>(f._2, f._1));
      JavaRDD<ITile> elevation = sparkContext.geoTiff("HYP_HR_SR");

      // 2- Perform a raptor join between the raster and vector data
      JavaRDD<RaptorJoinResult<int[]>> joinResults =
          RaptorJoin.raptorJoinIDFullJ(elevation, countries, new BeastOptions());

      // 3- Group the results by country ID to bring together all pixels.
      JavaPairRDD<Long, Iterable<Tuple3<Integer, Integer, int[]>>> countryPixels =
          joinResults.mapToPair(j -> new Tuple2<>(j.featureID(), new Tuple3<>(j.x(), j.y(), j.m()))).groupByKey();

      // 4- Build a lookup table that maps country ID to its name to use in naming output files
      Map<Long, String> countryNames =
          countries.mapToPair(c -> new Tuple2<>(c._1, c._2.getAs("NAME").toString())).collectAsMap();

      // 5- Put the pixels together into an image using the Java image API.
      countryPixels.foreach(value -> {
        List<Tuple3<Integer, Integer, int[]>> pixels = new ArrayList<>();
        int minX = Integer.MAX_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (Tuple3<Integer, Integer, int[]> pixel : value._2) {
          minX = Math.min(minX, pixel._1());
          maxX = Math.max(maxX, pixel._1());
          minY = Math.min(minY, pixel._2());
          maxY = Math.max(maxY, pixel._2());
          pixels.add(pixel);
        }

        BufferedImage image = new BufferedImage(maxX - minX + 1, maxY - minY + 1, BufferedImage.TYPE_INT_ARGB);
        for (Tuple3<Integer, Integer, int[]> pixel : pixels) {
          int x = pixel._1() - minX;
          int y = pixel._2() - minY;
          image.setRGB(x, y, new Color(pixel._3()[0], pixel._3()[1], pixel._3()[2]).getRGB());
        }
        // Write the image to the output
        Path imagePath = new Path("output-images", countryNames.get(value._1)+".png");
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