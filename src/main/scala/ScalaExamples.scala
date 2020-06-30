import edu.ucr.cs.bdlab.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.geolite.{EnvelopeND, Feature, IFeature, KVFeature}
import edu.ucr.cs.bdlab.util.UserOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Scala examples for Beast
 */
object ScalaExamples {
  def main(args: Array[String]): Unit = {
    import edu.ucr.cs.bdlab.sparkOperations._
    val conf = new SparkConf
    conf.setAppName("Beast Examples")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    // Initialize Spark context
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.setInt("mapred.max.split.size",8 * 1024 * 1024)

    val dataset1 = sc.spatialFile("diagonal_001.csv", "envelope", new UserOptions("separator:,"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val dataset2 = sc.spatialFile("gaussian_001.csv", "envelope", new UserOptions("separator:,"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    for (algorithm <- Array(ESJDistributedAlgorithm.BNLJ, ESJDistributedAlgorithm.PBSM)) {
      val t1 = System.nanoTime()
      val mbrCount = sc.longAccumulator("MBRCount")
      val results = dataset1.spatialJoin(dataset2, ESJPredicate.MBRIntersects, algorithm, mbrCount)
      val resultSize = results.count()
      val t2 = System.nanoTime()
      println(s"Algorithm $algorithm in ${(t2-t1)*1E-9} seconds and found $resultSize results and did ${mbrCount.value} mbr tests")
    }

    // Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
    val polygons = sc.shapefile("tl_2018_us_state.zip")

    // Load points in GeoJSON format. Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/index.geojson?mbr=-117.8538,33.2563,-116.8142,34.4099&point
    val points = sc.geojsonFile("Tweets_index.geojson")

    // Run a range query
    val range = new EnvelopeND(2, -117.337182, 33.622048, -117.241395, 33.72865)
    val matchedPolygons: RDD[IFeature] = polygons.rangeQuery(range)
    val matchedPoints: RDD[IFeature] = points.rangeQuery(range)

    // Run a spatial join operation between points and polygons (point-in-polygon) query
    val sjResults: RDD[(IFeature, IFeature)] =
      matchedPolygons.spatialJoin(matchedPoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)

    // Keep point coordinate, text, and state name
    val finalResults: RDD[IFeature] = sjResults.map(pip => {
      val polygon = pip._1
      val point = pip._2
      val feature = new Feature(point)
      feature.appendAttribute("state", polygon.getAttributeValue("NAME"))
      feature
    })

    // Write the output in CSV format
    finalResults.saveAsCSVPoints("output", 0, 1, ';')
  }
}
