# Zonal Statistics

This tutorial explains how to run the zonal statistics operation using geometries from a shapefile and raster data from HDF files.
The zonal statistics query takes the following as input:

* An aggregate function, e.g., sum, average, or maximum.
* A set of geometries, e.g., state or county boundaries.
* A raster file, e.g., temperature data.

It computes the aggregate function for each geometry on the raster data.
For example, it computes the average temperature for each state.

## Prerequisites

* Setup the development environment. [more](setup.md)
* Download some HDF files to play with. [More details](modis-download.md).
* Move all the downloaded raster files under the subfolder `raster/`.
* Download the [state boundaries file](ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/).
* Make sure all the downloaded files are in the current working directory.

## Steps

### 1. Initialize the Spark context
Initialize the Spark context to load the polygons.
```java
JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
UserOptions opts = new UserOptions();
```

### 2. Locate all dates for the raster data

Choose the folders that match a date range.
```java
String startDate = "2018.01.01";
String endDate = "2018.01.03";
Path rasterPath = new Path("raster");
FileSystem rFileSystem = rasterPath.getFileSystem(opts);
FileStatus[] matchingDates = rFileSystem.listStatus
    (rasterPath, HDF4Reader.createDateFilter(startDate, endDate));
```

### 3. Select all files under the matching dates
Under each folder, select the files that match the MBR of the polygons. Notice that if you load all the files, you will still get the same result but limiting the files based on the MBR will be faster.
```java
List<Path> allRasterFiles = new ArrayList<>();
for (FileStatus matchingDir : matchingDates) {
  FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath(),
      HDF4Reader.createTileIDFilter(new Rectangle2D.Double(mbr.minCoord[0],
          mbr.minCoord[1], mbr.getSideLength(0), mbr.getSideLength(1))));
  for (FileStatus p : matchingTiles)
    allRasterFiles.add(p.getPath());
}
```

### 4. Determine the CRS of the raster data to reproject the vector data
```java
HDF4Reader raster = new HDF4Reader();
raster.initialize(rFileSystem, allRasterFiles.get(0), "LST_Day_1km");
opts.set(SpatialInputFormat.TargetCRS, raster.getCRS().toWKT());
raster.close();
```

### 5. Load the polygons
Load the polygons from a shapefile and store in a list.
```java
JavaRDD<IFeature> polygons = SpatialReader.readInput(sc, opts, "tl_2018_us_state.zip", "shapefile");
List<IFeature> features = polygons.collect();
```

### 6. Initialize the input and output arrays
Create an array of `Statistics` as one for each polygon. These objects will be used to accumulate the results from all matching files.
```java
IGeometry[] geometries = new IGeometry[features.size()];
Statistics[] finalResults = new Statistics[features.size()];
for (int i = 0; i < features.size(); i++) {
  geometries[i] = features.get(i).getGeometry();
  finalResults[i] = new Statistics();
  finalResults[i].setNumBands(1);
}
```

### 7. Run the zonal statistics operation
Now, run the operation by processing all the polygons with all the matching raster files.
```java
HDF4Reader raster = new HDF4Reader();
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
```

### 8. Print out the results
Finally, print out the final results.
```java
System.out.println("Average Temperature (Kelvin)\tState Name");
for (int i = 0; i < geometries.length; i++) {
  if (finalResults[i].count[0] > 0) {
    System.out.printf("%f\t%s\n",
        finalResults[i].sum[0] / finalResults[i].count[0],
        features.get(i).getAttributeValue("NAME"));
  }
}
```

### Sample output
```
Average Temperature (Kelvin)	State Name
264.558095	Illinois
270.937360	Idaho
286.342519	New Mexico
289.839896	California
278.699085	Oregon
263.361604	Nebraska
274.040050	Washington
279.808268	Louisiana
279.798069	Utah
282.680895	Texas
...
```
## Complete code example

The entire code is shown below.
```java
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
import edu.ucr.cs.bdlab.geolite.IGeometry;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
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
    opts.set(SpatialInputFormat.TargetCRS, raster.getCRS().toWKT());
    raster.close();

    // 5. Load the polygons
    JavaRDD<IFeature> polygons = SpatialReader.readInput(sc, opts, "tl_2018_us_state.zip", "shapefile");
    List<IFeature> features = polygons.collect();

    // 6. Initialize the list of geometries and results array
    IGeometry[] geometries = new IGeometry[features.size()];
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
```