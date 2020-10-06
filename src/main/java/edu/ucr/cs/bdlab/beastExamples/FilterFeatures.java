/*
 * Copyright 2019 University of California, Riverside
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
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;


/**
 * Reads a Shapefile and filters the data in it using two methods.
 * See more details at
 * <a href="https://bitbucket.org/eldawy/beast-examples/src/master/doc/range-filter-shapefile.md">
 * https://bitbucket.org/eldawy/beast-examples/src/master/doc/range-filter-shapefile.md
 * </a>
 */
public class FilterFeatures {
  public static void main(String[] args) {
    try (JavaSpatialSparkContext sc = new JavaSpatialSparkContext("local[*]", "test")) {
      // Download the input file at
      // https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_airports.zip
      JavaRDD<IFeature> airports = sc.shapefile("ne_10m_airports.zip");
      System.out.printf("Total number of airports is %d\n", airports.count());

      Geometry range = new GeometryFactory().toGeometry(new Envelope(-128.1, -63.8, 27.3, 54.3));
      JavaRDD<IFeature> filtered_airports = JavaSpatialRDDHelper.rangeQuery(airports, range);
      System.out.printf("Number of loaded airports is %d\n", filtered_airports.count());
    }
  }
}
