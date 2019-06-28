package edu.ucr.cs.bdlab.beastExamples;


import edu.ucr.cs.bdlab.davinci.Canvas;
import edu.ucr.cs.bdlab.davinci.FastImageCanvas;
import edu.ucr.cs.bdlab.davinci.GeometricPlotter;
import edu.ucr.cs.bdlab.davinci.Plotter;
import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.Point;

import java.awt.Color;
import java.awt.Graphics2D;
import java.io.DataOutputStream;
import java.io.IOException;

@Plotter.Metadata(
    shortname = "myplot",
    description = "A plotter that draws the geometric shape of objects on a raster canvas"
)
public class NewPlotter extends GeometricPlotter {

  @Override
  public Canvas createCanvas(int width, int height, Envelope mbr) {
    return new FastImageCanvas(mbr, width, height);
  }

  @Override
  public void plot(Canvas canvasLayer, IFeature shape) {
    FastImageCanvas canvas = (FastImageCanvas) canvasLayer;
    Graphics2D g = canvas.getImage().createGraphics();
    double xscale = canvas.getImage().getWidth() / canvas.getInputMBR().getSideLength(0);
    double yscale = canvas.getImage().getHeight() / canvas.getInputMBR().getSideLength(1);
    int translateX = (int) (-canvas.getInputMBR().minCoord[0] * xscale);
    int translateY = (int) (-canvas.getInputMBR().minCoord[1] * yscale);
    g.translate(translateX, translateY);
    Point p = (Point) shape.getGeometry();
    int x = (int) (p.coords[0] * xscale);
    int y = (int) (p.coords[1] * yscale);
    if (shape.getAttributeValue("FULLNAME").toString().length() > 3) {
      g.setColor(Color.GREEN);
      g.fillOval(x - 2, y - 2, 5, 5);
    } else {
      g.setColor(Color.BLACK);
      g.fillRect(x, y, 1, 1);
    }
  }

}
