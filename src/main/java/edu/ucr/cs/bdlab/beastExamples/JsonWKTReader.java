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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import edu.ucr.cs.bdlab.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.geolite.Feature;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.io.FeatureReader;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.util.OperationParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;

/**
 * A custom record reader to use for a demo
 */
@FeatureReader.Metadata(
    description = "Parses a JSON file with one object per line and an attribute that encodes the geometry as WKT",
    shortName = "jsonwkt",
    extension = ".json"
)
public class JsonWKTReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(JsonWKTReader.class);

  @OperationParam(
      description = "The attribute name that contains the WKT-encoded geometry",
      defaultValue = "boundaryshape"
  )
  public static final String WKTAttribute = "wktattr";

  /**An underlying reader to read the input line-by-line*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**The name of the attribute that contains the WKT-encoded geometry*/
  protected String wktAttrName;

  /**The mutable Feature*/
  protected Feature feature;

  /**An optional attributed to filter the geometries in the input file*/
  protected EnvelopeND filterMBR;

  /**The underlying JSON parser*/
  protected final JsonFactory jsonFactory = new JsonFactory();

  protected final GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326);

  /**If the input contains wkt-encoded geometries, this is used to parse them*/
  protected final WKTReader wktReader = new WKTReader(geometryFactory);

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    lineReader.initialize(inputSplit, taskAttemptContext);
    Configuration conf = taskAttemptContext.getConfiguration();
    this.wktAttrName = conf.get(WKTAttribute, "boundaryshape");
    String filterMBRStr = conf.get(SpatialInputFormat.FilterMBR);
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] dblParts = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        dblParts[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new EnvelopeND(geometryFactory, dblParts.length/2, dblParts);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    feature = new Feature();
    // Read the line as text and parse it as JSON
    Text line = lineReader.getCurrentValue();
    JsonParser parser = jsonFactory.createParser(line.getBytes(), 0, line.getLength());
    // Iterate over key-value pairs
    if (parser.nextToken() == JsonToken.START_OBJECT) {
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String attrName = parser.getCurrentName();
        if (attrName.equals(wktAttrName)) {
          // Read the geometry
          String wkt = parser.nextTextValue();
          try {
            Geometry geometry = wktReader.read(wkt);
            feature.setGeometry(geometry);
          } catch (ParseException e) {
            throw new RuntimeException(String.format("Error parsing WKT '%s'", wkt), e);
          }
        } else {
          Object value = null;
          JsonToken token = parser.nextToken();
          switch (token) {
            case VALUE_NUMBER_FLOAT: value = parser.getFloatValue(); break;
            case VALUE_TRUE: value = true; break;
            case VALUE_FALSE: value = false; break;
            case VALUE_STRING: value = parser.getText(); break;
            case VALUE_NULL: value = null; break;
            case VALUE_NUMBER_INT: value = parser.getIntValue(); break;
            default:
              LOG.warn("Non-supported token value type " + token);
          }
          if (value != null)
            feature.appendAttribute(attrName, value);
        }
      }
    }
    return true;
  }

  @Override
  public EnvelopeND getCurrentKey() {
    return null;
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
