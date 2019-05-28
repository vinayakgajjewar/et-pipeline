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
import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.Feature;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.IGeometry;
import edu.ucr.cs.bdlab.io.FeatureReader;
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
import edu.ucr.cs.bdlab.util.OperationParam;
import edu.ucr.cs.bdlab.wktparser.ParseException;
import edu.ucr.cs.bdlab.wktparser.WKTParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  /**If the input contains wkt-encoded geometries, this is used to parse them*/
  protected final WKTParser wktParser = new WKTParser();

  /**An optional attributed to filter the geometries in the input file*/
  protected Envelope filterMBR;

  /**This flag signals the record reader to use immutable objects (a new object per record) which is useful with some
   * Spark operations that is not designed for mutable objects, e.g., partitionBy used in indexing*/
  protected boolean immutable;

  /**The underlying JSON parser*/
  protected final JsonFactory jsonFactory = new JsonFactory();

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    lineReader.initialize(inputSplit, taskAttemptContext);
    Configuration conf = taskAttemptContext.getConfiguration();
    this.wktAttrName = conf.get(WKTAttribute, "boundaryshape");
    this.immutable = conf.getBoolean(SpatialInputFormat.ImmutableObjects, false);
    String filterMBRStr = conf.get(SpatialInputFormat.FilterMBR);
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] dblParts = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        dblParts[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new Envelope(dblParts.length/2, dblParts);
    }
  }

  /**
   * An initializer that can be used outside the regular MapReduce context.
   * @param inputFile
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(Path inputFile, Configuration conf) throws IOException {
    FileSystem fileSystem = inputFile.getFileSystem(conf);
    FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
    BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    List<String> hosts = new ArrayList<String>();
    for (int i = 0; i < fileBlockLocations.length; i++)
      for (String host : fileBlockLocations[i].getHosts())
        hosts.add(host);
    FileSplit fileSplit = new FileSplit(inputFile, 0, fileStatus.getLen(), hosts.toArray(new String[hosts.size()]));
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    this.initialize(fileSplit, context);
  }

  protected Feature createValue() {
    return new Feature();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    if (feature == null || immutable)
      feature = createValue();
    else
      feature.clearAttributes();
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
            IGeometry geometry = wktParser.parse(wkt, feature.getGeometry());
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
  public Envelope getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public IFeature getCurrentValue() throws IOException, InterruptedException {
    return feature;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}
