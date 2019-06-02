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

import com.esri.core.geometry.Geometry;
import edu.ucr.cs.bdlab.davinci.GeometricPlotter;
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper;
import edu.ucr.cs.bdlab.davinci.Plotter;
import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.Feature;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.IGeometry;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.indexing.IndexerParams;
import edu.ucr.cs.bdlab.indexing.RSGrovePartitioner;
import edu.ucr.cs.bdlab.indexing.RTreeFeatureReader;
import edu.ucr.cs.bdlab.indexing.RTreeFeatureWriter;
import edu.ucr.cs.bdlab.indexing.SpatialPartitioner;
import edu.ucr.cs.bdlab.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.io.FeatureReader;
import edu.ucr.cs.bdlab.io.FeatureWriter;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.operations.Histogram;
import edu.ucr.cs.bdlab.operations.Index;
import edu.ucr.cs.bdlab.operations.MultilevelPlot;
import edu.ucr.cs.bdlab.operations.SparkSpatialPartitioner;
import edu.ucr.cs.bdlab.operations.SpatialJoin;
import edu.ucr.cs.bdlab.operations.SpatialReader;
import edu.ucr.cs.bdlab.operations.SpatialWriter;
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
public class IndexVisualize {

  @OperationParam(
      description = "Overwrite the output if it already exists {true, false}.",
      defaultValue = "false"
  )
  public static final String OverwriteOutput = "overwrite";

  public static void run(UserOptions opts, JavaSparkContext sc) throws IOException {
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
    JavaRDD<IFeature> input = SpatialReader.readInput(opts, opts.getInput(), sc);
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
    JavaPairRDD<Integer, IFeature> partitionedInput = Index.partitionFeatures(input, RSGrovePartitioner.class, opts);
    opts.setClass(SpatialOutputFormat.FeatureWriterClass, RTreeFeatureWriter.class, FeatureWriter.class);
    Index.saveIndex(partitionedInput, indexOutput, opts);

    // Now, build the visualization for the partitioned dataset
    opts.setBoolean(MultilevelPyramidPlotHelper.IncludeDataTiles, false);
    // Adjust the input format to read from the R-tree index correctly
    opts.set(SpatialInputFormat.InputFormat, "rtree");
    opts.setClass(SpatialInputFormat.FeatureReaderClass, RTreeFeatureReader.class, FeatureReader.class);
    // Create a full 20-level visualization
    MultilevelPyramidPlotHelper.Range levels = new MultilevelPyramidPlotHelper.Range(0, 19);
    // Start the actual visualization
    MultilevelPlot.plotFeatures(partitionedInput.values(), levels, GeometricPlotter.class, indexOutput, plotOutput, opts);
  }
}
