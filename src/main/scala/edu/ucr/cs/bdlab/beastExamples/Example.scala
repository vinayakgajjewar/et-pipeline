// Example.scala
// Dummy pipeline that uses fake data

package edu.ucr.cs.bdlab.beastExamples

// RDPro
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// My utility methods
import edu.ucr.cs.bdlab.beastExamples.SaturationVaporPressureSlope.computeApproxSaturationVaporPressureSlope

import scala.math.{exp, log, pow}

object Example {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val conf = new SparkConf().setAppName("Dummy Pipeline")
    conf.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    try {

      // Raster metadata for reshaping
      // TODO what values?
      val metadata = RasterMetadata.create(
        x1 = -50,
        y1 = 40,
        x2 = -60,
        y2 = 30,
        srid = 4326,
        rasterWidth = 10,
        rasterHeight = 10,
        tileWidth = 10,
        tileHeight = 10
      )

      // Load air temp data
      val T_pixels = sc.parallelize(Seq(
        (0, 0, 10.1f),
        (3, 4, 200.6f),
        (8, 9, 300.2f)
      ))
      val T = sc.rasterizePixels[Float](T_pixels, metadata)

      // Air
      // temp data from NARR is in K, so let's convert to degrees Celsius
      val T_converted = T.mapPixels(x => x - 272.15f)
      val T_min: Float = T_converted.flatten.map(_._4).min()
      val T_max: Float = T_converted.flatten.map(_._4).max()

      // Load relative humidity data
      // Relative humidity data given as %
      val RH_pixels = sc.parallelize(Seq(
        (0, 0, 2.7f),
        (3, 4, 2.2f),
        (8, 9, 3.8f)
      ))
      val RH = sc.rasterizePixels[Float](RH_pixels, metadata)
      val RH_min: Float = RH.flatten.map(_._4).min()
      val RH_max: Float = RH.flatten.map(_._4).max()

      // Load wind speed data
      // Units: m s^-1
      // 10 m above sea level
      val u_z_pixels = sc.parallelize(Seq(
        (0, 0, 561.4f),
        (3, 4, 230.7f),
        (8, 9, 766.5f)
      ))
      val u_z = sc.rasterizePixels[Float](u_z_pixels, metadata)

      // Load downward shortwave radiation flux data
      val R_s_pixels = sc.parallelize(Seq(
        (0, 0, 461.1f),
        (3, 4, 356.7f),
        (8, 9, 166.4f)
      ))
      val R_s = sc.rasterizePixels[Float](R_s_pixels, metadata)

      // Load R_nl
      // TODO: see if we need to do some calculations
      val R_nl_pixels = sc.parallelize(Seq(
        (0, 0, 468.4f),
        (3, 4, 756.6f),
        (8, 9, 151.2f)
      ))
      val R_nl = sc.rasterizePixels[Float](R_nl_pixels, metadata)

      // Equation 7
      // Compute atmospheric pressure (P) from elevation (z)
      // P is in kPa, z is in meters
      val z: Float = 10.0f // TODO we need actual data for z
      val P: Float = (101.3f * pow((293.0f - 0.0065f * z) / 293.0f, 5.26f)).toFloat

      // Equation 8
      // Compute psychometric constant (gamma) from atmospheric pressure
      // kPa / deg C
      val gamma: Float = (0.665f * pow(10.0f, -3) * P).toFloat

      // Equation 11
      // We are calculating saturation vapor pressure here
      // We need to calculate for both min and max temps
      val e_T_min: Float = (0.6108f * exp((17.27f * T_min) / (T_min + 237.3f))).toFloat
      val e_T_max: Float = (0.6108f * exp((17.27f * T_max) / (T_max + 237.3f))).toFloat

      // Equation 12
      // Calculate mean saturation vapor pressure
      val e_s: Float = ((e_T_max + e_T_min) / 2.0f).toFloat

      // Equation 13 TODO: update equation #
      // Here, we calculate Delta (slope of saturation vapor pressure curve)
      //val Delta: RasterRDD[Float] = T.mapPixels(x => (4098.0f * (0.6108f * exp((17.27f * x) / (x + 237.3f))) / pow(x + 237.3f, 2.0f)).toFloat)
      val Delta: RasterRDD[Float] = computeApproxSaturationVaporPressureSlope(T)

      // Equation 17
      // Here, we get actual vapor pressure (e_a) from relative humidity data
      // We are assuming that we have both RH_max and RH_min (as %)
      val e_a: Float = ((e_T_min * RH_max / 2 + e_T_max * RH_min / 2) / 2).toFloat

      // Equation 38
      // Compute net shortwave radiation from incoming solar radiation
      // Canopy reflection coefficient is defined as 23% for hypothetical grass reference crop
      val a = 0.23f
      val R_ns = R_s.mapPixels(x => (1 - a) * x)

      // Equation 39

      // Equation 40
      // Compute net radiation from incoming net shortwave radiation and outgoing net longwave radiation

      val R_n_overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
        R_ns,
        R_nl
      )
      val R_n: RasterRDD[Float] = R_n_overlay.mapPixels(x => x(0) - x(1))

      // Compute soil heat flux

      // For single-day and ten-day periods, soil heat flux is negligible
      // Equation 42
      val G = 0.0f

      // Equation 47
      // Compute wind speed at 2 meters given wind speed at height z
      // meters/second
      val u_2: RasterRDD[Float] = u_z.mapPixels(x => (x * 4.87f / log(67.8f * z - 5.42f)).toFloat)

      // Equation 6
      // FAO Penman-Monteith equation
      // Here we are finally computing reference evapotranspiration

      val ET_o_overlays_1: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
        Delta,
        u_2
      )
      val ET_o_overlays_2: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
        T,
        R_n
      )
      val ET_o_overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(ET_o_overlays_1, ET_o_overlays_2)
      val ET_o: RasterRDD[Float] = ET_o_overlay.mapPixels(x => (((0.408f * x(0) * (x(3) - G)) + (gamma * 900.0f * x(1) * (e_s - e_a) / (x(2) + 273.0f))) / (x(0) + gamma * (1.0f + 0.34f * x(1)))).toFloat)

      // Save output in GeoTIFF format
      ET_o.foreach(x => println(x.rasterMetadata.toString()))
      // TODO this does not work
      val outputPath: String = "file:///Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/output/ET_o"
      //ET_o.saveAsObjectFile(outputPath)
      //ET_o.saveAsGeoTiff(outputPath, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.BitsPerSample -> "8"))
      //ET_o.saveAsGeoTiff(outputPath)
      val reshaped_ET_o = RasterOperationsFocal.reshapeNN[Float](ET_o, metadata)
      reshaped_ET_o.saveAsGeoTiff(outputPath, Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.BitsPerSample -> "8"))

    } finally {
      spark.stop()
    }
  }
}