package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.IndexHelper.{Fixed, NumPartitions}
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RSGrovePartitioner}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Envelope, GeometryFactory}

/**
 * Scala examples for Beast
 */
object ScalaExamples {
  def main(args: Array[String]): Unit = {
    // 2A. Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext

    try {
      // 2B. Import Beast features
      import edu.ucr.cs.bdlab.beast._

      // 2C. Load spatial datasets
      // Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
      val polygons = sparkContext.shapefile("tl_2018_us_state.zip")
      // Load points in GeoJSON format. Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/index.geojson?mbr=-117.8538,33.2563,-116.8142,34.4099&point
      val points = sparkContext.geojsonFile("Tweets_index.geojson")

      // 2D. Run a range query
      val geometryFactory: GeometryFactory = new GeometryFactory()
      val range = geometryFactory.toGeometry(new Envelope(-117.337182, -117.241395, 33.622048, 33.72865))
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
      finalResults.saveAsCSVPoints(filename = "output", xColumn = 0, yColumn = 1, delimiter = ';')
    } finally {
      sparkSession.stop()
    }
  }
}
