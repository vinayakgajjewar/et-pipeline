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
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper;
import edu.ucr.cs.bdlab.beast.indexing.RSGrovePartitioner;
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.FeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.operations.FeatureWriterSizeFunction;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.OperationMetadata;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.davinci.GeometricPlotter;
import edu.ucr.cs.bdlab.davinci.MultilevelPlot;
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;

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
  public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) throws IOException {
    String indexOutput, plotOutput;
    plotOutput = inputs.length == 2? outputs[1] : inputs[0]+"_plot";
    indexOutput = outputs.length >= 1? outputs[0] : inputs[0]+"_index";
    boolean overwrite = opts.getBoolean(OverwriteOutput, false);
    Configuration conf = opts.loadIntoHadoopConf(sc.hadoopConfiguration());

    // Overwrite the output if it already exists
    if (overwrite) {
      Path path = new Path(plotOutput);
      FileSystem fileSystem = path.getFileSystem(conf);
      fileSystem.delete(path, true);

      path = new Path(indexOutput);
      fileSystem = path.getFileSystem(conf);
      fileSystem.delete(path, true);
    }

    // Read the features in the input dataset
    JavaRDD<IFeature> input = SpatialReader.readInput(sc, opts, inputs[0], opts.getString(SpatialInputFormat.InputFormat)).cache();
    // Write the summary
    Summary summary = Summary.computeForFeatures(input);
    JsonGenerator jsonGenerator = new JsonFactory().createGenerator(System.out);
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
    Summary.writeSummaryWithSchema(jsonGenerator, summary, input.first());
    jsonGenerator.close();

    // Reduce geometries to two dimensions to allow geometric plotter to work
    input = input.map(f -> {
      Geometry geom = f.getGeometry();
      // Reduce geometry coordinates to two dimensions
      if (geom instanceof PointND && ((PointND)geom).getCoordinateDimension() > 2) {
        geom = geom.getCentroid();
      } else if (geom instanceof EnvelopeND && ((EnvelopeND)geom).getCoordinateDimension() > 2) {
        geom = geom.getEnvelope();
      }
      return new Feature(geom);
    });

    // Index the file using R*-Grove as a global index and R-tree as a local index
    opts.setBoolean(IndexHelper.BalancedPartitioning(), true);
    opts.setBoolean(IndexHelper.DisjointIndex(), true);
    opts.set(SpatialOutputFormat.OutputFormat, "rtree");
    JavaPairRDD<Integer, IFeature> partitionedInput = IndexHelper.partitionFeatures(input, RSGrovePartitioner.class,
        new FeatureWriterSizeFunction(opts), opts);
    opts.setClass(SpatialOutputFormat.FeatureWriterClass, RTreeFeatureWriter.class, FeatureWriter.class);
    IndexHelper.saveIndex(partitionedInput, indexOutput, opts);

    // Now, build the visualization for the partitioned dataset
    opts.setBoolean(MultilevelPlot.IncludeDataTiles(), false);
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
