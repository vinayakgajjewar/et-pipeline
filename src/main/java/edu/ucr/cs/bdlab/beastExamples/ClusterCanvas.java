package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.davinci.Canvas;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.awt.Rectangle;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterCanvas extends Canvas {

  Map<Rectangle, Integer>  clusters=new ConcurrentHashMap<>();

  public ClusterCanvas() {}

  public ClusterCanvas(int width, int height, Envelope mbr) {
    super(mbr, width, height);
  }

  public void addPoint(Coordinate c, int radius, Envelope mbr) {
    double xscale = this.width / this.inputMBR.getWidth();
    double yscale = this.height / this.inputMBR.getHeight();
    int minx1 = (int)(mbr.getMinX() * xscale);
    int miny1 = (int)(mbr.getMinY() * yscale);
    int maxx1 = (int)(mbr.getMaxX() * xscale);
    int maxy1 = (int)(mbr.getMaxY() * yscale);
    if (maxx1 - minx1 < radius) {
      int diff = radius - (maxx1 - minx1);
      minx1 -= diff / 2;
      maxx1 = minx1 + radius;
    }
    if (maxy1 - miny1 < radius) {
      int diff = radius - (maxy1 - miny1);
      miny1 -= diff / 2;
      maxy1 = miny1 + radius;
    }
    Rectangle newCluster = new Rectangle(minx1, miny1, maxx1 - minx1, maxy1 - miny1);

    for (Rectangle cluster : clusters.keySet()) {
      if (cluster.intersects(newCluster)) {
        clusters.put(cluster, clusters.get(cluster) + 1);
        return;
      }
    }
    clusters.put(newCluster, 1);
  }

  public void mergeCanvas(ClusterCanvas intermediateLayer) {
    for(Map.Entry<Rectangle, Integer> newCluster : intermediateLayer.clusters.entrySet()) {
      boolean merged = false;
      for (Rectangle existingCluster : clusters.keySet()) {
        if (existingCluster.intersects(newCluster.getKey())) {
          clusters.put(existingCluster, clusters.get(existingCluster) + newCluster.getValue());
          merged = true;
          break;
        }
      }
      if (!merged) {
        clusters.put(newCluster.getKey(), newCluster.getValue());
      }
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    int count = clusters.isEmpty()? 0 : clusters.size();
    out.writeInt(count);
    for(Map.Entry<Rectangle, Integer> i : clusters.entrySet()) {
      out.writeInt(i.getKey().x);
      out.writeInt(i.getKey().y);
      out.writeInt(i.getKey().width);
      out.writeInt(i.getKey().height);
      out.writeInt(i.getValue());
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    super.readExternal(in);
    int count = in.readInt();

    for(int i=0;i<count;i++) {
      int X=in.readInt();
      int Y=in.readInt();
      int W=in.readInt();
      int H=in.readInt();
      int c= in.readInt();
      Rectangle p=new Rectangle(X, Y, W, H);
      clusters.put(p, c);
    }
  }

  public int getWidth() {
    return width;
  }

  public int getHeight() {
    return height;
  }

}
