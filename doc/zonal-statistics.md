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

## States dataset

   <!-- CSS and JS files need to be added once to the head element of your page. Check if you have them already -->
   <link rel='stylesheet' href='https://openlayers.org/en/v5.3.0/css/ol.css' type='text/css'>
   <script language='javascript' src='https://code.jquery.com/jquery-3.3.1.min.js'></script>
   <script src='https://cdn.rawgit.com/openlayers/openlayers.github.io/master/en/v5.3.0/build/ol.js'></script>
   <script src='https://cdn.polyfill.io/v2/polyfill.min.js?features=requestAnimationFrame,Element.prototype.classList,URL'></script>
   <script language='javascript' src='https://davinci.cs.ucr.edu/static/davinci.js'></script>
   <!-- The following code displays the desired map initialized to a customized location -->
   <!-- Apply any desired CSS customization to the top div element -->
   <div class='davinci map' data-center='39.9307,-95.7615' data-zoom='4'>
    <a class='davinci layer' data-url='TIGER2018/STATE_plot'></a>
   </div>
  

## Steps

### 1. Initialize the Spark context

    JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
    UserOptions opts = new UserOptions();

### 2. Load the polygons
    JavaRDD<IFeature> polygons = SpatialReader.readInput(sc, opts, "tl_2018_us_state.zip", "shapefile");
    List<IFeature> features = polygons.collect();

### 3. Locate all dates for the raster data

    String startDate = "2018.01.01";
    String endDate = "2018.01.03";
    Path rasterPath = new Path("raster");
    FileSystem rFileSystem = rasterPath.getFileSystem(opts);
    FileStatus[] matchingDates = rFileSystem.listStatus
        (rasterPath, HDF4Reader.createDateFilter(startDate, endDate));

### 4. Select all files under the matching dates

    List<Path> allRasterFiles = new ArrayList<>();
    for (FileStatus matchingDir : matchingDates) {
      FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath());
      for (FileStatus p : matchingTiles)
        allRasterFiles.add(p.getPath());
    }


### 5. Initialize the input and output arrays

    IGeometry[] geometries = new IGeometry[features.size()];
    Statistics[] finalResults = new Statistics[features.size()];
    for (int i = 0; i < features.size(); i++) {
      geometries[i] = features.get(i).getGeometry();
      finalResults[i] = new Statistics();
      finalResults[i].setNumBands(1);
    }


### 6. Run the zonal statistics operation

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

### 7. Print out the results

    System.out.println("Average Temperature (Kelvin)\tState Name");
    for (int i = 0; i < geometries.length; i++) {
      if (finalResults[i].count[0] > 0) {
        System.out.printf("%f\t%s\n",
            finalResults[i].sum[0] / finalResults[i].count[0],
            features.get(i).getAttributeValue("NAME"));
      }
    }

### Sample output

    Average Temperature (Kelvin)	State Name
    282.232612	West Virginia
    283.571412	Illinois
    271.370946	Minnesota
    285.591091	Maryland
    271.485000	Idaho
    283.950000	Delaware
    288.265999	New Mexico
    285.593056	California
    ...

## Complete code example

The entire code is shown below.

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
     */
    public class ZonalStatisticsExample {
    
      public static void main(String[] args) throws IOException {
        // 1. Create a default SparkContext
        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
        UserOptions opts = new UserOptions();
    
        // 2. Load the polygons
        JavaRDD<IFeature> polygons = SpatialReader.readInput(sc, opts, "tl_2018_us_state.zip", "shapefile");
        List<IFeature> features = polygons.collect();
    
        // 3. Locate all dates for the raster data
        String startDate = "2018.01.01";
        String endDate = "2018.01.03";
        Path rasterPath = new Path("raster");
        FileSystem rFileSystem = rasterPath.getFileSystem(opts);
        FileStatus[] matchingDates = rFileSystem.listStatus
            (rasterPath, HDF4Reader.createDateFilter(startDate, endDate));
    
        // 4. Select all files under the matching dates
        List<Path> allRasterFiles = new ArrayList<>();
        for (FileStatus matchingDir : matchingDates) {
          FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath());
          for (FileStatus p : matchingTiles)
            allRasterFiles.add(p.getPath());
        }
    
        // 5. Initialize the list of geometries and results array
        IGeometry[] geometries = new IGeometry[features.size()];
        Statistics[] finalResults = new Statistics[features.size()];
        for (int i = 0; i < features.size(); i++) {
          geometries[i] = features.get(i).getGeometry();
          finalResults[i] = new Statistics();
          finalResults[i].setNumBands(1);
        }
    
        // 6. Run the zonal statistics operation
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
    
        // 7. Print out the results
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
