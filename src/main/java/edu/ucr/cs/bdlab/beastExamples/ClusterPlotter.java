package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.davinci.Canvas;
import edu.ucr.cs.bdlab.davinci.Plotter;
import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.util.OperationParam;
import org.apache.hadoop.conf.Configuration;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
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
  public void setup(Configuration clo) {
    super.setup(clo);
    this.radius = clo.getInt(Radius, 50);
  }

  @Override
  public Canvas createCanvas(int width, int height, Envelope mbr) {
    return new ClusterCanvas(width, height, mbr);
  }

  @Override
  public boolean plot(Canvas canvasLayer, IFeature shape) {
    Point point = new Point(2);
    Envelope mbr=new Envelope(2);
    shape.getGeometry().envelope(mbr);
    shape.getGeometry().centroid(point);
    ClusterCanvas canvas = (ClusterCanvas) canvasLayer;
    canvas.addPoint(point,radius,mbr);
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
    double xscale = clusterCanvas.getWidth() / clusterCanvas.getInputMBR().getSideLength(0);
    double yscale = clusterCanvas.getHeight() / clusterCanvas.getInputMBR().getSideLength(1);
    int xtranslate = (int) (clusterCanvas.getInputMBR().minCoord[0] * xscale);
    int ytranslate = (int) (clusterCanvas.getInputMBR().minCoord[1] * yscale);

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
  public void write(DataOutput out) throws IOException {
    out.writeInt(radius);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.radius = in.readInt();
  }
}
