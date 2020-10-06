/*
 * Copyright 2018 University of California, Riverside
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

import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.raptor.HDF4Reader;
import edu.ucr.cs.bdlab.raptor.Statistics;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a simple zonal statistics operation.
 * For further instructions check:
 * https://bitbucket.org/eldawy/beast-examples/src/master/doc/zonal-statistics.md
 */
public class ZonalStatisticsExample {

  public static void main(String[] args) throws IOException {
    // 1. Create a default SparkContext
    JavaSpatialSparkContext ssc = new JavaSpatialSparkContext("local[*]", "test");
    BeastOptions opts = new BeastOptions();

    // 2. Locate all dates for the raster data
    String startDate = "2018.01.01";
    String endDate = "2018.01.03";
    Path rasterPath = new Path("raster");
    FileSystem rFileSystem = rasterPath.getFileSystem(opts.loadIntoHadoopConf(ssc.hadoopConfiguration()));
    FileStatus[] matchingDates = rFileSystem.listStatus
            (rasterPath, HDF4Reader.createDateFilter(startDate, endDate));

    // 3. Select all files under the matching dates
    List<String> allRasterFiles = new ArrayList<>();
    for (FileStatus matchingDir : matchingDates) {
      FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath());
      for (FileStatus p : matchingTiles)
        allRasterFiles.add(p.getPath().toString());
    }

    // 4. Determine the CRS of the raster data to reproject the vector data
    HDF4Reader raster = new HDF4Reader();
    raster.initialize(rFileSystem, new Path(allRasterFiles.get(0)), "LST_Day_1km");
    CoordinateReferenceSystem rasterCRS = raster.getCRS();
    raster.close();

    // 5. Load the polygons and project to the raster CRS
    JavaRDD<IFeature> polygons = ssc.shapefile("tl_2018_us_state.zip");
    polygons = JavaSpatialRDDHelper.reproject(polygons, rasterCRS);

    //ø 6. Join with the raster files
    opts.set("RasterReader.RasterLayerID", "LST_Day_1km");
    JavaRDD<Tuple5<IFeature, Integer, Integer, Integer, Float>> joinResults =
        JavaSpatialRDDHelper.raptorJoin(polygons, allRasterFiles.toArray(new String[0]), opts);

    // 7. Compute the join results
    JavaPairRDD<String, Tuple4<Integer, Integer, Integer, Float>> groupedResults =
        joinResults.mapToPair(x -> new Tuple2<>((String) x._1().getAttributeValue("NAME"), new Tuple4<>(x._2(), x._3(), x._4(), x._5())));
    Statistics initialSate = new Statistics();
    initialSate.setNumBands(1);
    JavaPairRDD<String, Statistics> statsResults = groupedResults.aggregateByKey(initialSate,
        (st, x) -> (Statistics) st.collect(x._2(), x._3(), new float[] {x._4()}),
        (st1, st2) -> (Statistics) st1.accumulate(st2));
    List<Tuple2<String, Statistics>> finalResults = statsResults.sortByKey().collect();

    // 8. Print out the results
    System.out.println("State Name\tAverage Temperature (Kelvin)");
    for (Tuple2<String, Statistics> entry : finalResults)
      System.out.printf("%s\t%f\n", entry._1(), entry._2().sum[0] / entry._2().count[0]);
    // Clean up
    ssc.close();
  }
}
