# Add a new FeatureReader

This tutorial describes how to create a new FeatureReader which reads geometric features from a custom input format.
The FeatureReader is nothing but a Hadoop `RecordReader` which reads pairs of `<Envelope, IFeature>`
where the key is the minimum-bounding rectangle (MBR) of the record and the value is the spatial feature.
You can find some examples in the code such as `GeoJSONFeatureReader` and `CSVFeatureReader`.
This tutorial implements a new feature reader that reads trajectory data from the [GeoLife dataset]
(https://star.cs.ucr.edu/?GeoLife#center=40.17,117.33&zoom=5).

## 1. Create a new class

Create a new class that extends `FeatureReader`.

    @FeatureReader.Metadata(
        description = "Parses GeoLife .plt files",
        shortName = "geolife",
        extension = ".plt"
    )
    public class GeoLifeReader extends FeatureReader {}
    
The annotation `FeatureReader.Metadata` is required and it makes it easier to use the new feature reader.
Further, the `extension` attribute allows Beast to autodetect the feature reader based on the extension
if the user does not explicitly provide the input format.

## 2. Implement the `initialize` function.

The initialize function takes an `InputSplit` and initializes a reader that read this split.
The split is usually a `FileSplit` that identifies all or a part of a file.
The following implementation mainly initializes an underlying `LineReader` that reads the input
text file line-by-line.

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      lineReader.initialize(split, context);
      this.immutableFeatures = context.getConfiguration().getBoolean(SpatialInputFormat.ImmutableObjects, false);
    }

The flag `immutableFeatures` is important. This flag is automatically set to `true` in Spark or when object reuse
is not possible. When this flag is set, the method `nextKeyValue` should always create a new key and value
and should not reuse the objects. If not set, then objects can be reused to improve the performance.

## 3. Implement the `nextKeyValue` method

Implement (override) the method `public boolean nextKeyValue()` which should read the next record from the
input and stores it internally to be returned by the two methods `IFeature getCurrentKey()`
and `IFeature getCurrentValue()`. According to the format of GeoLife files, the following function skips the
first six lines of the file (header) and then returns one point feature for each consecutive line.
Once the end-of-file is reached, this method should return false.

    @Override
    public boolean nextKeyValue() throws IOException {
      if (!lineReader.nextKeyValue())
        return false;
      if (lineReader.getCurrentKey().get() == 0) {
        // Skip the first six lines
        for (int $i = 0; $i < 6; $i++) {
          if (!lineReader.nextKeyValue())
            return false;
        }
      }
      if (immutableFeatures || feature == null) {
        feature = new CSVFeature(new Point(2));
        featureMBR = new Envelope(2);
      }
      Text value = lineReader.getCurrentValue();
      try {
        double longitude = Double.parseDouble(CSVFeature.deleteAttribute(value, ',', 1,
            CSVFeatureReader.DefaultQuoteCharacters));
        double latitude = Double.parseDouble(CSVFeature.deleteAttribute(value, ',', 0,
            CSVFeatureReader.DefaultQuoteCharacters));
        ((Point)feature.getGeometry()).set(longitude, latitude);
        feature.setFieldSeparator((byte) ',');
        feature.setFieldValues(value.toString());
        feature.getGeometry().envelope(featureMBR);
        return true;
      } catch (Exception e) {
        LOG.error(String.format("Error reading object at %d with value '%s'", lineReader.getCurrentKey().get(), value));
        throw e;
      }
    }

## 4. Implement the `getCurrentKey` and `getCurrentValue` functions


    public Envelope getCurrentKey() { return featureMBR; }
    public IFeature getCurrentValue() { return feature; }


## 5. Implement the `close` method

This method just closes the underlying line reader to free up the resources.

    public void close() throws IOException { lineReader.close(); }

## 6. Add your new reader to the configuration files

To make your new reader accessible to all Beast functions, including the command-line interface,
you need to add the full name of your class to the configuration file `beast.xml`

    <Readers>
      <Reader>edu.ucr.cs.bdlab.beastExamples.GeoLifeReader</Reader>
    </Readers>

## How to use your new writer

Let's say you want to compute the summary of an input file in the new format.
You can download all the data from the (GeoLife website)
[https://www.microsoft.com/en-us/research/project/geolife-building-social-networks-using-human-location-history/#!downloads]

You can compile your code into JAR using the following command:

    mvn package

Let's say the generated JAR file is called `beast-example.jar`.
Now you can run the following command:

```shell script
spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.5.0-RC1 \
    --class edu.ucr.cs.bdlab.sparkOperations.Main beast-example.jar \
    summary 20090504203925.plt iformat:geolife
```

If the input file has the right extension `.plt`, you can also use the auto-detect feature as follows.

```shell script
spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.5.0-RC1 \
    --class edu.ucr.cs.bdlab.sparkOperations.Main beast-example.jar \
    summary 20090504203925.plt iformat:*auto*
```
You should see an output that looks like the following.
```json
{
  "extent" : [ 116.319719, 39.999697, 116.327532, 40.007537 ],
  "size" : 5515,
  "num_features" : 79,
  "num_points" : 79,
  "avg_sidelength" : [ 0.0, 0.0 ],
  "attributes" : [ {
    "name" : "attribute#0",
    "type" : "string"
  }, {
    "name" : "attribute#1",
    "type" : "string"
  }, {
    "name" : "attribute#2",
    "type" : "string"
  }, {
    "name" : "attribute#3",
    "type" : "string"
  }, {
    "name" : "attribute#4",
    "type" : "string"
  } ]
}
```
## Full source code
You can find the full source code in the beast-examples repository in the class `GeoLifeReader`.
The full source code is provided below for your reference.
```java
/*
 * Copyright 2020 University of California, Riverside
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
import edu.ucr.cs.bdlab.io.SpatialInputFormat;
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
  protected Envelope featureMBR;

  /**When this flag is set to true, a new object is created for each feature.*/
  protected boolean immutableFeatures;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    lineReader.initialize(split, context);
    this.immutableFeatures = context.getConfiguration().getBoolean(SpatialInputFormat.ImmutableObjects, false);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    if (lineReader.getCurrentKey().get() == 0) {
      // Skip the first six lines
      for (int $i = 0; $i < 6; $i++) {
        if (!lineReader.nextKeyValue())
          return false;
      }
    }
    if (immutableFeatures || feature == null) {
      feature = new CSVFeature(new Point(2));
      featureMBR = new Envelope(2);
    }
    Text value = lineReader.getCurrentValue();
    try {
      double longitude = Double.parseDouble(CSVFeature.deleteAttribute(value, ',', 1,
          CSVFeatureReader.DefaultQuoteCharacters));
      double latitude = Double.parseDouble(CSVFeature.deleteAttribute(value, ',', 0,
          CSVFeatureReader.DefaultQuoteCharacters));
      ((Point)feature.getGeometry()).set(longitude, latitude);
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
```