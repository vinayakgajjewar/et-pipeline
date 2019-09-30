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

import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.io.CSVFeature;
import edu.ucr.cs.bdlab.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.io.FeatureReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Reads GeoLife .mplot files as a sequence of points. Similar to the CSVFeatureReader but it skips the first six
 * lines per the GeoLife format.
 */
@FeatureReader.Metadata(
    description = "Parses GeoLife .mplot files",
    shortName = "geolife",
    extension = ".plt"
)
public class GeoLifeReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(GeoLifeReader.class);

  /**An underlying reader for the text file*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**The returned feature*/
  protected CSVFeature feature;

  /**The MBR of the current feature*/
  protected Envelope featureMBR = new Envelope(2);

  /**A flag that is raised after the first six lines in the header are skipped*/
  protected boolean headerSkipped;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    lineReader.initialize(split, context);
    headerSkipped = false;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!headerSkipped) {
      // Skip the first six lines
      for (int $i = 0; $i < 6; $i++) {
        if (!lineReader.nextKeyValue())
          return false;
      }
      headerSkipped = true;
    }
    if (!lineReader.nextKeyValue())
      return false;
    Text value = lineReader.getCurrentValue();
    try {
      double longitude = Double.parseDouble(CSVFeature.deleteAttribute(value, ',', 1,
          CSVFeatureReader.DefaultQuoteCharacters));
      double latitude = Double.parseDouble(CSVFeature.deleteAttribute(value, ',', 0,
          CSVFeatureReader.DefaultQuoteCharacters));
      feature = new CSVFeature(new Point(longitude, latitude));
      feature.setFieldSeparator((byte) ',');
      feature.setFieldValues(value.toString());
      feature.getGeometry().envelope(featureMBR);
      return true;
    } catch (Exception e) {
      LOG.error(String.format("Error reading object at %d with value '%s'", lineReader.getCurrentKey().get(), value));
      throw e;
    }
  }

  @Override
  public Envelope getCurrentKey() {
    return featureMBR;
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
