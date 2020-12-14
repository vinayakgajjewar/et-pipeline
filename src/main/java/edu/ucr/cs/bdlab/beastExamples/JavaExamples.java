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
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Beast example that uses the Java interface
 */
public class JavaExamples {
  public static void main(String[] args) {
    // Initialize Spark
    SparkConf conf = new SparkConf().setAppName("Beast Example");

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]");

    // Create Spark session (for Dataframe API) and Spark context (for RDD API)
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

    try {
      // Load spatial datasets
      // Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
      JavaRDD<IFeature> polygons = SpatialReader.readInput(sparkContext, new BeastOptions(), "tl_2018_us_state.zip", "shapefile");

      // Load points in GeoJSON format. Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/data_index.csv.gz?mbr=-117.8538,33.2563,-116.8142,34.4099&point
      JavaRDD<IFeature> points = SpatialReader.readInput(sparkContext, new BeastOptions(), "Tweets_index.geojson", "geojson");

      // Run a range query
      GeometryFactory geometryFactory = new GeometryFactory();
      Geometry range = geometryFactory.toGeometry(new Envelope(-117.337182, -117.241395, 33.622048, 33.72865));
      JavaRDD<IFeature> matchedPolygons = JavaSpatialRDDHelper.rangeQuery(polygons, range);
      JavaRDD<IFeature> matchedPoints = JavaSpatialRDDHelper.rangeQuery(points, range);

      // Run a spatial join operation between points and polygons (point-in-polygon) query
      JavaPairRDD<IFeature, IFeature> sjResults = JavaSpatialRDDHelper.spatialJoin(matchedPolygons, matchedPoints,
          SpatialJoinAlgorithms.ESJPredicate.Contains, SpatialJoinAlgorithms.ESJDistributedAlgorithm.PBSM);

      // Keep point coordinate, text, and state name
      JavaRDD<IFeature> finalResults = sjResults.map(pip -> {
        IFeature polygon = pip._1;
        IFeature point = pip._2;
        Feature feature = new Feature(point);
        feature.appendAttribute("state", polygon.getAttributeValue("NAME"));
        return feature;
      });

      // Write the output in CSV format
      JavaSpatialRDDHelper.saveAsCSVPoints(finalResults, "output", 0, 1, ';', true);
    } finally {
      // Clean up Spark session
      sparkSession.stop();
    }
  }
}
