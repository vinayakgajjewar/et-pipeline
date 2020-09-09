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

import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.io.Reprojector;
import edu.ucr.cs.bdlab.raptor.Collector;
import edu.ucr.cs.bdlab.raptor.HDF4Reader;
import edu.ucr.cs.bdlab.raptor.Statistics;
import edu.ucr.cs.bdlab.raptor.ZonalStatistics;
import edu.ucr.cs.bdlab.sparkOperations.SpatialReader;
import edu.ucr.cs.bdlab.util.UserOptions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

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
    JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
    UserOptions opts = new UserOptions();

    // 2. Locate all dates for the raster data
    String startDate = "2018.01.01";
    String endDate = "2018.01.03";
    Path rasterPath = new Path("raster");
    FileSystem rFileSystem = rasterPath.getFileSystem(opts);
    FileStatus[] matchingDates = rFileSystem.listStatus
            (rasterPath, HDF4Reader.createDateFilter(startDate, endDate));

    // 3. Select all files under the matching dates
    List<Path> allRasterFiles = new ArrayList<>();
    for (FileStatus matchingDir : matchingDates) {
      FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath());
      for (FileStatus p : matchingTiles)
        allRasterFiles.add(p.getPath());
    }

    // 4. Determine the CRS of the raster data to reproject the vector data
    HDF4Reader raster = new HDF4Reader();
    raster.initialize(rFileSystem, allRasterFiles.get(0), "LST_Day_1km");
    CoordinateReferenceSystem rasterCRS = raster.getCRS();
    raster.close();

    // 5. Load the polygons
    JavaRDD<IFeature> polygons = SpatialReader.readInput(sc, opts, "tl_2018_us_state.zip", "shapefile");
    polygons = polygons.map(f -> {
      Geometry g = f.getGeometry();
      g = Reprojector.reprojectGeometry(g, rasterCRS);
      f.setGeometry(g);
      return f;
    });
    List<IFeature> features = polygons.collect();

    // 6. Initialize the list of geometries and results array
    Geometry[] geometries = new Geometry[features.size()];
    Statistics[] finalResults = new Statistics[features.size()];
    for (int i = 0; i < features.size(); i++) {
      geometries[i] = features.get(i).getGeometry();
      finalResults[i] = new Statistics();
      finalResults[i].setNumBands(1);
    }

    // 7. Run the zonal statistics operation
    for (Path rasterFile : allRasterFiles) {
      raster.initialize(rFileSystem, rasterFile, "LST_Day_1km");
      Collector[] stats = ZonalStatistics.computeZonalStatisticsScanline(raster, geometries, Statistics.class);
      // Merge the results
      for (int i = 0; i < stats.length; i++) {
        if (stats[i] != null)
          finalResults[i].accumulate(stats[i]);
      }
      raster.close();
    }

    // 8. Print out the results
    System.out.println("Average Temperature (Kelvin)\tState Name");
    for (int i = 0; i < geometries.length; i++) {
      if (finalResults[i].count[0] > 0) {
        System.out.printf("%f\t%s\n",
            finalResults[i].sum[0] / finalResults[i].count[0],
            features.get(i).getAttributeValue("NAME"));
      }
    }

    // Clean up
    sc.close();
  }
}
