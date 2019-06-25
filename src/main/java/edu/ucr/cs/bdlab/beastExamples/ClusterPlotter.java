package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.geolite.Envelope;
import edu.ucr.cs.bdlab.geolite.GeometryCollection;
import edu.ucr.cs.bdlab.geolite.IFeature;
import edu.ucr.cs.bdlab.geolite.IGeometry;
import edu.ucr.cs.bdlab.geolite.Point;
import edu.ucr.cs.bdlab.geolite.twod.LineString2D;
import edu.ucr.cs.bdlab.geolite.twod.MultiLineString2D;
import edu.ucr.cs.bdlab.geolite.twod.MultiPolygon2D;
import edu.ucr.cs.bdlab.geolite.twod.Polygon2D;
import edu.ucr.cs.bdlab.util.OperationParam;
import org.apache.hadoop.conf.Configuration;
import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * A plotter that draws the geometry of the features (e.g., the point location or polygon boundary)
 */
@Plotter.Metadata(
    shortname = "cgplot",
    description = "..."
)
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
        //System.out.println("Radius:"+radius);
    }

    @Override
    public Canvas createCanvas(int width, int height, Envelope mbr) {
        return new ClusterCanvas(width, height, mbr);
    }

    @Override
    public void plot(Canvas canvasLayer, IFeature shape) {
        Point point = new Point(2);
        Envelope mbr=new Envelope(2);
        shape.getGeometry().envelope(mbr);
        shape.getGeometry().centroid(point);
        //System.out.println("Shape:"+shape+"Point:"+point);
        ClusterCanvas canvas = (ClusterCanvas) canvasLayer;
        //radius=50;
        canvas.addPoint(point,radius,mbr);
    }

    @Override
    public Class<? extends Canvas> getCanvasClass() {
        return ClusterCanvas.class;
    }

    @Override
    public void merge(Canvas finalLayer, Canvas intermediateLayer) {
        // System.out.println("finalLayer:");
        ((ClusterCanvas)finalLayer).mergeCanvas((ClusterCanvas) intermediateLayer);
        //  System.out.println("Centers:"+((ClusterCanvas) finalLayer).clusters.keySet());
    }

    @Override
    public void writeImage(Canvas layer, DataOutputStream out, boolean vflip) throws IOException {
        ClusterCanvas clusterCanvas =  (ClusterCanvas)layer;
        double xscale = clusterCanvas.width / clusterCanvas.inputMBR.getSideLength(0);
        double yscale = clusterCanvas.height / clusterCanvas.inputMBR.getSideLength(1);
        int xtranslate = (int) (clusterCanvas.inputMBR.minCoord[0] * xscale);
        int ytranslate = (int) (clusterCanvas.inputMBR.minCoord[1] * yscale);

        BufferedImage finalImage = new BufferedImage(clusterCanvas.width, clusterCanvas.height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = finalImage.createGraphics();
        g.setColor(Color.BLACK);

        for(Map.Entry<Rectangle, Integer> i:clusterCanvas.clusters.entrySet()) {
            int x = i.getKey().x - xtranslate;
            int y = i.getKey().y - ytranslate;
            int w = i.getKey().width;
            int h = i.getKey().height;
            x = Math.max(x, 0);
            y = Math.max(y, 0);
            if (x + w > clusterCanvas.width) {
                x = clusterCanvas.width - w;
            }
            if (y + h > clusterCanvas.height) {
                y = clusterCanvas.height - h;
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
