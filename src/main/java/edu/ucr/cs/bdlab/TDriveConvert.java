package edu.ucr.cs.bdlab;

import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.geolite.twod.MultiLineString2D;
import edu.ucr.cs.bdlab.io.CSVFeature;
import edu.ucr.cs.bdlab.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.operations.SpatialReader;
import edu.ucr.cs.bdlab.operations.SpatialWriter;
import edu.ucr.cs.bdlab.util.UserOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Plots T-Drive data
 *
 */

public class App {
  public static void main(String[] args) throws IOException {
    // Initialize spark context
    SparkConf conf = new SparkConf();
    conf.setAppName("plot-tdrive");
    conf.setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    // Read the input as a CSV file with points where the x and y columns are attributes #2 and #3
    UserOptions opts = new UserOptions();
    SpatialInputFormat.setInputFormat(opts, "point(2,3)");;
    opts.set(CSVFeatureReader.FieldSeparator, ",");
    JavaRDD<IFeature> points = SpatialReader.readInput(opts, args[0], sc);
    // Group by trajectory ID
    JavaPairRDD<Integer, Iterable<IFeature>> groupedPoints = points.groupBy(f -> Integer.parseInt((String) f.getAttributeValue(0)));
    // Convert each list of points to a linestring
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    JavaRDD<IFeature> linestrings = groupedPoints.map(x -> {
      List<IFeature> sortedPoints = new ArrayList();
      for (IFeature f : x._2)
        sortedPoints.add(f);
      // Sort by time
      sortedPoints.sort(Comparator.comparing(f -> ((String) f.getAttributeValue(1))));
      // Add to the linestring in order
      MultiLineString2D linestring = new MultiLineString2D();
      long t_1 = 0;
      for (IFeature point : sortedPoints) {
        Point p = (Point) point.getGeometry();
        long timestamp = simpleDateFormat.parse((String)point.getAttributeValue(1)).getTime();
        // If the time difference is more than 15 minutes, start a new section
        if (t_1 != 0 && timestamp - t_1 > 15 * 60 * 1000)
          linestring.endCurrentLineString();
        linestring.addPoint(p.coords[0], p.coords[1]);
        t_1 = timestamp;
      }
      String trajectoryID = (String) sortedPoints.get(0).getAttributeValue(0);
      String timestampStart = (String) sortedPoints.get(0).getAttributeValue(1);
      String timestampEnd = (String) sortedPoints.get(sortedPoints.size() - 1).getAttributeValue(1);
      return new CSVFeature(linestring, new String[] {trajectoryID, timestampStart, timestampEnd}, ',');
    });

    opts.set(CSVFeatureWriter.FieldSeparator, "\t");
    SpatialWriter.saveFeatures(linestrings, "wkt(3)", args[1], opts);
  }
}
