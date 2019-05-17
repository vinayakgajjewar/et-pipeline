package edu.ucr.cs.bdlab.star;

import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.geolite.twod.LineString2D;
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
  protected CSVFeature feature;

  /**The MBR of the current feature*/
  protected Envelope featureMBR = new Envelope(2);

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    lineReader.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (!lineReader.nextKeyValue())
      return false;
    Text value = lineReader.getCurrentValue();
    try {
      double x1 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 2));
      double y1 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 2));
      double x2 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 3));
      double y2 = Double.parseDouble(CSVFeatureReader.deleteAttribute(value, ',', 3));
      LineString2D lineString = new LineString2D();
      lineString.addPoint(x1, y1);
      lineString.addPoint(x2, y2);
      feature = new CSVFeature(lineString);
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
