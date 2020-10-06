# Zonal Statistics

This tutorial explains how to run the zonal statistics operation using geometries from a Shapefile and raster data from HDF files.
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
JavaSpatialSparkContext ssc = new JavaSpatialSparkContext("local[*]", "test");
```

### 2. Locate all dates for the raster data

Choose the folders that match a date range.
```java
String startDate = "2018.01.01";
String endDate = "2018.01.03";
Path rasterPath = new Path("raster");
FileSystem rFileSystem = rasterPath.getFileSystem(ssc.hadoopConfiguration());
FileStatus[] matchingDates = rFileSystem.listStatus(rasterPath,
    HDF4Reader.createDateFilter(startDate, endDate));
```

### 3. Select all files under the matching dates
Under each folder, select the files that match the MBR of the polygons. Notice that if you load all the files, you will still get the same result but limiting the files based on the MBR will be faster.
```java
List<String> allRasterFiles = new ArrayList<>();
for (FileStatus matchingDir : matchingDates) {
  FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath());
  for (FileStatus p : matchingTiles)
    allRasterFiles.add(p.getPath().toString());
}
```

### 4. Determine the CRS of the raster data to reproject the vector data
```java
HDF4Reader raster = new HDF4Reader();
raster.initialize(rFileSystem, allRasterFiles.get(0), "LST_Day_1km");
opts.set(SpatialInputFormat.TargetCRS, raster.getCRS().toWKT());
raster.close();
```

### 5. Load the polygons and project to the raster CRS
Load the polygons from a shapefile and project them to the CRS of the raster file.
```java
JavaRDD<IFeature> polygons = ssc.shapefile("tl_2018_us_state.zip");
polygons = JavaSpatialRDDHelper.reproject(polygons, rasterCRS);
```

### 6. Join with the raster files
Perform a Raptor join (Raster-Plus-Vector) between the polygons and the raster files.
```java
BeastOptions opts = new BeastOptions();
opts.set("RasterReader.RasterLayerID", "LST_Day_1km");
JavaRDD<Tuple5<IFeature, Integer, Integer, Integer, Float>> joinResults =
    JavaSpatialRDDHelper.raptorJoin(polygons, allRasterFiles.toArray(new String[0]), opts);
```

### 7. Compute the join results
Group the join results by state and aggregate into the statistics we wish to compute.
```java
JavaPairRDD<String, Tuple4<Integer, Integer, Integer, Float>> groupedResults =
    joinResults.mapToPair(x -> new Tuple2<>((String) x._1().getAttributeValue("NAME"),
        new Tuple4<>(x._2(), x._3(), x._4(), x._5())));
Statistics initialSate = new Statistics();
initialSate.setNumBands(1);
JavaPairRDD<String, Statistics> statsResults = groupedResults.aggregateByKey(initialSate,
    (st, x) -> (Statistics) st.collect(x._2(), x._3(), new float[] {x._4()}),
    (st1, st2) -> (Statistics) st1.accumulate(st2));
List<Tuple2<String, Statistics>> finalResults = statsResults.sortByKey().collect();
```

### 8. Print out the results
Finally, print out the final results.
```java
System.out.println("State Name\tAverage Temperature (Kelvin)");
    for (Tuple2<String, Statistics> entry : finalResults)
      System.out.printf("%s\t%f\n", entry._1(), entry._2().sum[0] / entry._2().count[0]);
```

### Sample output
```
State Name	Average Temperature (Kelvin)
Arizona	294.025580
California	289.839896
Colorado	278.488132
Idaho	270.937360
Illinois	264.558095
Iowa	258.511743
Kansas	282.125599
Louisiana	279.808268
Missouri	261.739170
Montana	261.561770
Nebraska	263.361604
Nevada	284.456589
New Mexico	286.342519
North Dakota	259.158064
...
```
## Complete code example

The entire code is shown below.
```java
/*
 * Copyright 2020 University of California, Riverside
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

    // 2. Locate all dates for the raster data
    String startDate = "2018.01.01";
    String endDate = "2018.01.03";
    Path rasterPath = new Path("raster");
    FileSystem rFileSystem = rasterPath.getFileSystem(ssc.hadoopConfiguration());
    FileStatus[] matchingDates = rFileSystem.listStatus(rasterPath, HDF4Reader.createDateFilter(startDate, endDate));

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

    // 6. Join with the raster files
    BeastOptions opts = new BeastOptions();
    opts.set("RasterReader.RasterLayerID", "LST_Day_1km");
    JavaRDD<Tuple5<IFeature, Integer, Integer, Integer, Float>> joinResults =
        JavaSpatialRDDHelper.raptorJoin(polygons, allRasterFiles.toArray(new String[0]), opts);

    // 7. Compute the join results
    JavaPairRDD<String, Tuple4<Integer, Integer, Integer, Float>> groupedResults =
        joinResults.mapToPair(x -> new Tuple2<>((String) x._1().getAttributeValue("NAME"),
            new Tuple4<>(x._2(), x._3(), x._4(), x._5())));
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
```