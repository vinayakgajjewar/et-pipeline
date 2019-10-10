package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.sparkOperations.SpatialReader;
import edu.ucr.cs.bdlab.util.UserOptions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Reads a Shapefile and filters the data in it using two methods.
 * See more details at
 * <a href="https://bitbucket.org/eldawy/beast-examples/src/master/doc/range-filter-shapefile.md">
 * https://bitbucket.org/eldawy/beast-examples/src/master/doc/range-filter-shapefile.md
 * </a>
 */
public class FilterFeatures {
  public static void main(String[] args) {
    try (JavaSparkContext sc = new JavaSparkContext("local[*]", "test")) {
      // Download the input file at
      // https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_airports.zip
      UserOptions opts = new UserOptions();
      JavaRDD<IFeature> airports = SpatialReader.readInput(sc, opts, "ne_10m_airports.zip", "shapefile");
      System.out.printf("Total number of airports is %d\n", airports.count());

      opts.set(SpatialInputFormat.FilterMBR, "-128.1,27.3,-63.8,54.3");
      JavaRDD<IFeature> filtered_airports = SpatialReader.readInput(sc, opts, "ne_10m_airports.zip", "shapefile");
      System.out.printf("Number of loaded airports is %d\n", filtered_airports.count());

      Envelope range = new Envelope(2, -128.1, 27.3, -63.8, 54.3);
      JavaRDD<IFeature> filtered_airports2 = airports.filter(f -> range.contains(f.getGeometry()));
      System.out.printf("Number of filtered airports is %d\n", filtered_airports2.count());
    }
  }
}
