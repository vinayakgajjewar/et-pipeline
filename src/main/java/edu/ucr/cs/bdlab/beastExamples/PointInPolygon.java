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

import edu.ucr.cs.bdlab.geolite.Feature;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.operations.SpatialJoin;
import edu.ucr.cs.bdlab.operations.SpatialReader;
import edu.ucr.cs.bdlab.operations.SpatialWriter;
import edu.ucr.cs.bdlab.util.OperationMetadata;
import edu.ucr.cs.bdlab.util.OperationParam;
import edu.ucr.cs.bdlab.util.UserOptions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * An operation that finds the points that are inside each polygon and writes the output as
 * polygon attributes (without geometry), point attributes, and point geometry
 */
@OperationMetadata(
    shortName =  "pip",
    description = "Computes the point-in-polygon query between two inputs.",
    inputArity = "2",
    outputArity = "1",
    inheritParams = {SpatialInputFormat.class})
public class PointInPolygon {
  @OperationParam(
      description = "Overwrite the output if it already exists {true, false}.",
      defaultValue = "false"
  )
  public static final String OverwriteOutput = "overwrite";

  public static void run(UserOptions opts, JavaSparkContext sc) throws IOException {
    // Read the input features for the two datasets
    JavaRDD<IFeature> polygons = SpatialReader.readInput(opts, opts.getInputs()[0], 0, sc);
    JavaRDD<IFeature> points = SpatialReader.readInput(opts, opts.getInputs()[1], 1, sc);

    // Compute the spatial join
    JavaPairRDD<IFeature, IFeature> joinsResults = SpatialJoin.spatialJoinBNLJ(polygons, points, SpatialJoin.JoinPredicate.CONTAINS);

    // Combine the results into features while removing the polygon geometry and keeping only its attributes
    JavaRDD<Feature> results = joinsResults.map(pair -> {
      IFeature polygon = pair._1;
      IFeature point = pair._2;
      Feature result = new Feature(point.getGeometry());
      for (int $iAttr = 0; $iAttr < polygon.getNumAttributes(); $iAttr++)
        result.appendAttribute(polygon.getAttributeName($iAttr), polygon.getAttributeValue($iAttr));
      for (int $iAttr = 0; $iAttr < point.getNumAttributes(); $iAttr++)
        result.appendAttribute(point.getAttributeName($iAttr), point.getAttributeValue($iAttr));
      return result;
    });

    // Write to the output
    int numPolygonAttributes = polygons.first().getNumAttributes();
    // The output format is CSV where the point is encoded as WKT right between polygon and point attributes
    String oFormat = String.format("wkt(%d)", numPolygonAttributes);
    opts.set(CSVFeatureWriter.FieldSeparator, ";");
    SpatialWriter.saveFeatures(results, oFormat, opts.getOutput(), opts);
  }
}
