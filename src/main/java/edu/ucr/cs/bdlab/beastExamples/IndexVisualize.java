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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import edu.ucr.cs.bdlab.davinci.GeometricPlotter;
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper;
import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.Feature;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.IGeometry;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.indexing.IndexerParams;
import edu.ucr.cs.bdlab.indexing.RSGrovePartitioner;
import edu.ucr.cs.bdlab.indexing.RTreeFeatureReader;
import edu.ucr.cs.bdlab.indexing.RTreeFeatureWriter;
import edu.ucr.cs.bdlab.io.FeatureReader;
import edu.ucr.cs.bdlab.io.FeatureWriter;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.sparkOperations.GeometricSummary;
import edu.ucr.cs.bdlab.sparkOperations.Index;
import edu.ucr.cs.bdlab.sparkOperations.JCLIOperation;
import edu.ucr.cs.bdlab.sparkOperations.MultilevelPlot;
import edu.ucr.cs.bdlab.sparkOperations.SpatialReader;
import edu.ucr.cs.bdlab.stsynopses.Summary;
import edu.ucr.cs.bdlab.util.OperationMetadata;
import edu.ucr.cs.bdlab.util.OperationParam;
import edu.ucr.cs.bdlab.util.UserOptions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Indexes the given input dataset using a two-level index and then visualizes it using the GeometricPlotter
 * in 20 levels while using the index for non-materialized tiles.
 */
@OperationMetadata(
    shortName =  "xviz",
    description = "Indexes the data and then visualizes it using adaptive multilevel plotter",
    inputArity = "1",
    outputArity = "0-2",
    inheritParams = {SpatialInputFormat.class})
public class IndexVisualize implements JCLIOperation {

  @OperationParam(
      description = "Overwrite the output if it already exists {true, false}.",
      defaultValue = "false"
  )
  public static final String OverwriteOutput = "overwrite";

  @Override
  public Object run(UserOptions opts, JavaSparkContext sc) throws IOException {
    String indexOutput, plotOutput;
    plotOutput = opts.getNumOutputs() == 2? opts.getOutputs()[1] : opts.getInput()+"_plot";
    indexOutput = opts.getNumOutputs() >= 1? opts.getOutputs()[0] : opts.getInput()+"_index";
    boolean overwrite = opts.getBoolean(OverwriteOutput, false);

    // Overwrite the output if it already exists
    if (overwrite) {
      Path path = new Path(plotOutput);
      FileSystem fileSystem = path.getFileSystem(opts);
      fileSystem.delete(path, true);

      path = new Path(indexOutput);
      fileSystem = path.getFileSystem(opts);
      fileSystem.delete(path, true);
    }

    // Read the features in the input dataset
    JavaRDD<IFeature> input = SpatialReader.readInput(sc, opts).cache();
    // Write the summary
    Summary summary = GeometricSummary.computeForFeaturesJ(input);
    JsonGenerator jsonGenerator = new JsonFactory().createGenerator(System.out);
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
    GeometricSummary.writeDatasetSchema(jsonGenerator, summary, input.first());
    jsonGenerator.close();

    // Reduce geometries to two dimensions to allow geometric plotter to work
    input = input.map(f -> {
      IGeometry geom = f.getGeometry();
      if (geom.getCoordinateDimension() > 2) {
        // Reduce geometry coordinates to two dimensions
        switch (geom.getType()) {
          case POINT:
            Point point = (Point) geom;
            geom = new Point(point.coords[0], point.coords[1]);
            break;
          case ENVELOPE:
            Envelope envelope = (Envelope) geom;
            geom = new Envelope(2, envelope.minCoord[0], envelope.minCoord[1], envelope.maxCoord[0], envelope.maxCoord[1]);
            break;
          default:
            throw new RuntimeException(String.format("Cannot reduce the dimensions of geometries of type '%s'", geom.getType()));
        }
      }
      return new Feature(geom);
    });

    // Index the file using R*-Grove as a global index and R-tree as a local index
    opts.setBoolean(IndexerParams.BalancedPartitioning, true);
    opts.setBoolean(IndexerParams.DisjointIndex, true);
    opts.set(SpatialOutputFormat.OutputFormat, "rtree");
    JavaPairRDD<Integer, IFeature> partitionedInput = Index.partitionFeatures(input, RSGrovePartitioner.class, opts);
    opts.setClass(SpatialOutputFormat.FeatureWriterClass, RTreeFeatureWriter.class, FeatureWriter.class);
    Index.saveIndex(partitionedInput, indexOutput, opts);

    // Now, build the visualization for the partitioned dataset
    opts.setBoolean(MultilevelPyramidPlotHelper.IncludeDataTiles, false);
    // Adjust the input format to read from the R-tree index correctly
    opts.set(SpatialInputFormat.InputFormat, "rtree");
    // Create a full 20-level visualization
    MultilevelPyramidPlotHelper.Range levels = new MultilevelPyramidPlotHelper.Range(0, 19);
    // Start the actual visualization
    MultilevelPlot.plotFeatures(partitionedInput.values(), levels.min, levels.max, GeometricPlotter.class, indexOutput,
        plotOutput, opts);
    return null;
  }
}
