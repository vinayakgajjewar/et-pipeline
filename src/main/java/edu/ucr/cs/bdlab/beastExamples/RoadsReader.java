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

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.IOException;

/**
 * Reads the road network dataset provided by SpatialHadoop on the following link
 * https://drive.google.com/file/d/0B1jY75xGiy7ecDEtR1V1X21QVkE
 *
 */
@FeatureReader.Metadata(
    description = "Parses a CSV file that contains line segments",
    shortName = "roadnetwork",
    extension = ".csv"
)
public class RoadsReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(RoadsReader.class);

  /**An underlying reader for the text file*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**The returned feature*/
  protected Feature feature;

  protected GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326);

  @Override
  public void initialize(InputSplit split, Configuration conf) throws IOException {
    lineReader.initialize(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()));
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    Text value = lineReader.getCurrentValue();
    try {
      double x1 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 2, CSVFeatureReader.DefaultQuoteCharacters));
      double y1 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 2, CSVFeatureReader.DefaultQuoteCharacters));
      double x2 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 3, CSVFeatureReader.DefaultQuoteCharacters));
      double y2 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 3, CSVFeatureReader.DefaultQuoteCharacters));
      Coordinate[] coordinates = {
              new CoordinateXY(x1, y1),
              new CoordinateXY(x2, y2)
      };
      LineString lineString = factory.createLineString(coordinates);
      feature = new Feature(lineString);
      int i = 0;
      while (value.getLength() > 0)
        feature.appendAttribute("attr"+i, CSVFeatureReader.deleteAttribute(value, ',', 0, CSVFeatureReader.DefaultQuoteCharacters));
      return true;
    } catch (Exception e) {
      LOG.error(String.format("Error reading object at %d with value '%s'", lineReader.getCurrentKey().get(), value));
      throw e;
    }
  }

  @Override
  public IFeature getCurrentValue() {
    return feature;
  }

  @Override
  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}
