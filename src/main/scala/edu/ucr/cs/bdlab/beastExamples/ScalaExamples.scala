package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, Feature, IFeature}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.GeometryFactory

/**
 * Scala examples for Beast
 */
object ScalaExamples {
  def main(args: Array[String]): Unit = {
    // 2A. Initialize Spark context

    val conf = new SparkConf
    conf.setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 2B. Import Beast features
    import edu.ucr.cs.bdlab.beast._

    // 2C. Load spatial datasets
    // Use a smaller Split size if the memory on your executor machines is low
    sc.hadoopConfiguration.setInt("mapred.max.split.size",8 * 1024 * 1024)

    // Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
    val polygons = sc.shapefile("tl_2018_us_state.zip")

    // Load points in GeoJSON format. Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/index.geojson?mbr=-117.8538,33.2563,-116.8142,34.4099&point
    val points = sc.geojsonFile("Tweets_index.geojson")

    // 2D. Run a range query
    val geometryFactory: GeometryFactory = new GeometryFactory()
    val range = new EnvelopeND(geometryFactory, 2, -117.337182, 33.622048, -117.241395, 33.72865)
    val matchedPolygons: RDD[IFeature] = polygons.rangeQuery(range)
    val matchedPoints: RDD[IFeature] = points.rangeQuery(range)

    // 2E. Run a spatial join operation between points and polygons (point-in-polygon) query
    val sjResults: RDD[(IFeature, IFeature)] =
      matchedPolygons.spatialJoin(matchedPoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)

    // 2F. Keep point coordinate, text, and state name
    val finalResults: RDD[IFeature] = sjResults.map(pip => {
      val polygon = pip._1
      val point = pip._2
      val feature = new Feature(point)
      feature.appendAttribute("state", polygon.getAttributeValue("NAME"))
      feature
    })

    // Write the output in CSV format
    finalResults.saveAsCSVPoints(filename="output", xColumn = 0, yColumn = 1, delimiter = ';')
  }
}
