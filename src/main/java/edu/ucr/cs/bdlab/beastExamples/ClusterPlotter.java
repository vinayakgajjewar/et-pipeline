package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.davinci.Canvas;
import edu.ucr.cs.bdlab.davinci.Plotter;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Map;

/**
 * A plotter that draws the geometry of the features (e.g., the point location or polygon boundary)
 */
@Plotter.Metadata(
    shortname = "cgplot", description = "...", imageExtension = ".png")
public class ClusterPlotter extends Plotter {

  @OperationParam(
      description = "Radius for clustering",
      defaultValue = "50"
  )
  public static final String Radius = "radius";
  private int radius;

  @Override
  public void setup(BeastOptions clo) {
    super.setup(clo);
    this.radius = clo.getInt(Radius, 50);
  }

  @Override
  public Canvas createCanvas(int width, int height, Envelope mbr) {
    return new ClusterCanvas(width, height, mbr);
  }

  @Override
  public boolean plot(Canvas canvasLayer, IFeature shape) {
    Envelope mbr = shape.getGeometry().getEnvelopeInternal();
    Coordinate centroid = shape.getGeometry().getCentroid().getCoordinate();
    ClusterCanvas canvas = (ClusterCanvas) canvasLayer;
    canvas.addPoint(centroid, radius, mbr);
    return true;
  }

  @Override
  public Class<? extends Canvas> getCanvasClass() {
    return ClusterCanvas.class;
  }

  @Override
  public Canvas merge(Canvas finalLayer, Canvas intermediateLayer) {
    ((ClusterCanvas)finalLayer).mergeCanvas((ClusterCanvas) intermediateLayer);
    return finalLayer;
  }

  @Override
  public void writeImage(Canvas layer, OutputStream out, boolean vflip) throws IOException {
    ClusterCanvas clusterCanvas =  (ClusterCanvas)layer;
    double xscale = clusterCanvas.getWidth() / clusterCanvas.getInputMBR().getWidth();
    double yscale = clusterCanvas.getHeight() / clusterCanvas.getInputMBR().getHeight();
    int xtranslate = (int) (clusterCanvas.getInputMBR().getMinX() * xscale);
    int ytranslate = (int) (clusterCanvas.getInputMBR().getMinY() * yscale);

    BufferedImage finalImage = new BufferedImage(clusterCanvas.getWidth(), clusterCanvas.getHeight(),
        BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = finalImage.createGraphics();
    g.setColor(Color.BLACK);

    for(Map.Entry<Rectangle, Integer> i:clusterCanvas.clusters.entrySet()) {
      int x = i.getKey().x - xtranslate;
      int y = i.getKey().y - ytranslate;
      int w = i.getKey().width;
      int h = i.getKey().height;
      x = Math.max(x, 0);
      y = Math.max(y, 0);
      if (x + w > clusterCanvas.getWidth()) {
        x = clusterCanvas.getWidth() - w;
      }
      if (y + h > clusterCanvas.getHeight()) {
        y = clusterCanvas.getHeight() - h;
      }
      g.drawOval(x, y, w, h);
      g.drawString(Integer.toString(i.getValue()), x + w / 2, y + h / 2);
    }
    ImageIO.write(finalImage, "png", out);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(radius);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    this.radius = in.readInt();
  }

}
