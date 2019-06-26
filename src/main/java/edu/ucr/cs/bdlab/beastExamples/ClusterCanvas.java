package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.davinci.Canvas;
import edu.ucr.cs.bdlab.geolite.Envelope;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterCanvas extends Canvas {

  Map<Rectangle, Integer>  clusters=new ConcurrentHashMap<>();

  public ClusterCanvas() {}

  public ClusterCanvas(int width, int height, Envelope mbr) {
    super(mbr, width, height);
  }

  public void addPoint(edu.ucr.cs.bdlab.geolite.Point p, int radius, Envelope mbr) {
    double xscale = this.width / this.inputMBR.getSideLength(0);
    double yscale = this.height / this.inputMBR.getSideLength(1);
    int minx1 = (int)(mbr.minCoord[0] * xscale);
    int miny1 = (int)(mbr.minCoord[1] * yscale);
    int maxx1 = (int)(mbr.maxCoord[0] * xscale);
    int maxy1 = (int)(mbr.maxCoord[1] * yscale);
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
  public void write(DataOutput out) throws IOException {
    super.write(out);
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
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
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
