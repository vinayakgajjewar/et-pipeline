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
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialWriter;
import edu.ucr.cs.bdlab.beast.util.OperationMetadata;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;

/**
 * An operation that finds the points that are inside each polygon and writes the output as
 * polygon attributes (without geometry), point attributes, and point geometry
 */
@OperationMetadata(
    shortName =  "pip",
    description = "Computes the point-in-polygon query between two inputs. The two inputs are <polygons>, <points>.",
    inputArity = "2",
    outputArity = "1",
    inheritParams = {SpatialFileRDD.class})
public class PointInPolygon implements JCLIOperation {
  @OperationParam(
      description = "Overwrite the output if it already exists {true, false}.",
      defaultValue = "false"
  )
  public static final String OverwriteOutput = "overwrite";

  @Override
  public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) throws IOException {
    JavaSpatialSparkContext ssc = new JavaSpatialSparkContext(sc.sc());
    // Read the input features for the two datasets
    JavaRDD<IFeature> polygons = ssc.spatialFile(inputs[0], opts.retainIndex(0));
    JavaRDD<IFeature> points = ssc.spatialFile(inputs[1], opts.retainIndex(1));

    // Compute the spatial join
    JavaPairRDD<IFeature, IFeature> joinsResults = JavaSpatialRDDHelper.spatialJoin(polygons, points);

    // Combine the results into features while removing the polygon geometry and keeping only its attributes
    JavaRDD<IFeature> results = joinsResults.map(pair -> {
      IFeature polygon = pair._1;
      IFeature point = pair._2;
      Geometry geometry = point.getGeometry();
      Object[] values = new Object[point.length() + polygon.length() - 2];
      point.toSeq().copyToArray(values);
      polygon.toSeq().copyToArray(values, point.length() - 1);
      return Feature.create(geometry, null, null, values);
    });

    // Write to the output
    int numPolygonAttributes = polygons.first().length() - 1;
    // The output format is CSV where the point is encoded as WKT right between polygon and point attributes
    String oFormat = String.format("wkt(%d)", numPolygonAttributes);
    opts.set(CSVFeatureWriter.FieldSeparator, ";");
    if (opts.getBoolean(OverwriteOutput, false)) {
      Path outPath = new Path(outputs[0]);
      FileSystem fileSystem = outPath.getFileSystem(opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
      if (fileSystem.exists(outPath))
        fileSystem.delete(outPath, true);
    }
    SpatialWriter.saveFeaturesJ(results, oFormat, outputs[0], opts);
    return null;
  }
}
