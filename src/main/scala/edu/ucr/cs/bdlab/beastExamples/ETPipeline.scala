// ETPipeline.scala
// Uses the Penman-Monteith method to calculate evapotranspiration using NARR data.

// Units and variables:
// https://www.emc.ncep.noaa.gov/mmb/rreanl/narr_archive_contents.pdf

// Data source:
// https://psl.noaa.gov/data/gridded/data.narr.html

package edu.ucr.cs.bdlab.beastExamples

// RDPro
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.raptor.{GeoTiffWriter, RasterOperationsFocal, RasterOperationsLocal}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties

/*
 * Utility modules for computing intermediate results.
 */
import edu.ucr.cs.bdlab.beastExamples.SaturationVaporPressureSlope.computeApproxSaturationVaporPressureSlope
import edu.ucr.cs.bdlab.beastExamples.NetRadiation.computeNetRadiation
import edu.ucr.cs.bdlab.beastExamples.PenmanMonteith.computeLatentHeatFlux
import edu.ucr.cs.bdlab.beastExamples.AerodynamicResistance.computeAerodynamicResistance
import edu.ucr.cs.bdlab.beastExamples.BulkSurfaceResistance.computeBulkSurfaceResistance
import edu.ucr.cs.bdlab.beastExamples.InstantaneousET.computeInstantaneousET
import edu.ucr.cs.bdlab.beastExamples.SaturationVaporPressure.computeSaturationVaporPressure
import edu.ucr.cs.bdlab.beastExamples.SensibleHeatFlux.computeSensibleHeatFlux

import scala.math.{exp, log, pow}

object ETPipeline {

  /*
   * Compute latent heat flux using the Penman-Monteith method.
   */
  def main(args: Array[String]): Unit = {

    // Open configuration file.
    val propsUrl: String = "/Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/application.properties"
    val properties: Properties = new Properties()
    properties.load(scala.io.Source.fromFile(propsUrl).bufferedReader())

    // Initialize Spark
    val sparkAppName : String = properties.getProperty("spark_app_name")
    val sparkAppMaster : String = properties.getProperty("spark_app_master")
    val conf = new SparkConf().setAppName(sparkAppName)
    conf.setMaster(sparkAppMaster)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    try {

      // Raster metadata for reshaping
      // TODO what values?
      /*val metadata = RasterMetadata.create(
        x1 = -50,
        y1 = 40,
        x2 = -60,
        y2 = 30,
        srid = 4326,  // Northern Lambert Conformal Conic.
        rasterWidth = 349,
        rasterHeight = 277,
        tileWidth = 10,
        tileHeight = 10
      )*/

      // Load air temp data
      val T_path : String = properties.getProperty("T_path")
      var T = sc.geoTiff[Array[Float]](T_path)
      print("Count:")
      println(T.count())
      //T = RasterOperationsFocal.reshapeNN(T, metadata)

      // Just grab the first layer for now
      // TODO: decide what layer we need to use
      val T_first : RasterRDD[Float] = T.mapPixels(x => x(0))
      print("Count:")
      println(T_first.count())

      // Air temp data from NARR is in K, so let's convert to degrees Celsius
      val T_converted = T_first.mapPixels(x => x - 272.15f)
      val T_min: Float = T_converted.flatten.map(_._4).min()
      val T_max: Float = T_converted.flatten.map(_._4).max()

      // Load relative humidity data
      // Relative humidity data given as %
      val RH_path : String = properties.getProperty("RH_path")
      val RH = sc.geoTiff[Array[Float]](RH_path)
      val RH_first : RasterRDD[Float] = RH.mapPixels(x => x(0))
      val RH_max = RH_first.flatten.map(_._4).max()
      val RH_min = RH_first.flatten.map(_._4).min()

      // Load wind speed data
      // Units: m s^-1
      // 10 m above sea level
      val u_z_path: String = properties.getProperty("u_z_path")
      val u_z_all = sc.geoTiff[Array[Float]](u_z_path)

      // Just grab the first layer for now.
      // TODO decide what layer we want to use.
      val u_z: RasterRDD[Float] = u_z_all.mapPixels(x => x(0))

      // Load downward shortwave radiation flux data
      val R_s_path: String = properties.getProperty("R_s_path")
      val R_s_all = sc.geoTiff[Array[Float]](R_s_path)
      val R_s: RasterRDD[Float] = R_s_all.mapPixels(x => x(0))

      // Load R_nl
      // TODO: see if we need to do some calculations
      // TODO I should probably rename this variable at some point.
      val R_nl_path: String = properties.getProperty("R_nl_path")
      val R_nl_all = sc.geoTiff[Array[Float]](R_s_path)
      val R_nl: RasterRDD[Float] = R_nl_all.mapPixels(x => x(0))

      // Equation 7
      // Compute atmospheric pressure (P) from elevation (z)
      // P is in kPa, z is in meters
      val z: Float = 10.0f // TODO not sure if we want to hardcode this
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
      val e_s: Float = (e_T_max + e_T_min) / 2.0f

      // Equation 13 TODO: update equation #
      // Here, we calculate Delta (slope of saturation vapor pressure curve)
      val Delta: RasterRDD[Float] = computeApproxSaturationVaporPressureSlope(T_first)

      // Equation 17
      // Here, we get actual vapor pressure (e_a) from relative humidity data
      // We are assuming that we have both RH_max and RH_min (as %)
      val e_a: Float = (e_T_min * RH_max / 2 + e_T_max * RH_min / 2) / 2

      /*
       * Load surface albedo data from NARR.
       */
      val alpha_path: String = properties.getProperty("alpha_path")
      val alpha_all: RasterRDD[Array[Float]] = sc.geoTiff[Array[Float]](alpha_path)
      val alpha: RasterRDD[Float] = alpha_all.mapPixels(x => x(0))

      /*
       * Load upward longwave radiation flux data from NARR (W m^-1).
       */
      val R_lu_path: String = properties.getProperty("R_lu_path")
      val R_lu_all: RasterRDD[Array[Float]] = sc.geoTiff[Array[Float]](R_lu_path)
      val R_lu: RasterRDD[Float] = R_lu_all.mapPixels(x => x(0))

      /*
       * Compute net radiation using my external function.
       */
      val R_n: RasterRDD[Float] = computeNetRadiation(
        R_s,
        R_nl,
        R_lu,
        alpha
      )

      // Compute soil heat flux

      // For single-day and ten-day periods, soil heat flux is negligible
      // Equation 42
      /*
       * TODO compute soil heat flux using the equation given in the METRIC 2007 paper
       * Equation 26 in Allen et al. 2007
       */
      val G: Float = 0.0f

      /*
       * Compute aerodynamic resistance using eq. 14 in the Dhungel et al. 2014 paper.
       */
      val r_ah: RasterRDD[Float] = computeAerodynamicResistance(
        Z_om, // Estimate this as 0.005
        u_z,
        0, /* TODO idk what this should be */
        10
      )

      /*
       * Load air temperature data at 2 meters.
       */
      val T_a_path: String = properties.getProperty("T_a_path")
      val T_a_all: RasterRDD[Array[Float]] = sc.geoTiff[Array[Float]](T_a_path)
      val T_a: RasterRDD[Float] = T_a_all.mapPixels(x => x(0))

      /*
       * Compute sensible heat flux (H) using equation (A15) of Dhungel et al. 2014.
       */
      val H: RasterRDD[Float] = computeSensibleHeatFlux(
        T_first,
        T_a,  // TODO see if we need to convert units
        r_ah
      )

      /*
       * Compute instantaneous ET using equations (1) and (52) of Allen et al. 2007.
       */
      val ET_inst: RasterRDD[Float] = computeInstantaneousET(
        T_first,
        R_n,
        G,
        H
      )

      /*
       * Compute saturation vapor pressure of air (e_o_air).
       * TODO: make sure we're using the right input for T_a.
       */
      val e_o_air: RasterRDD[Float] = computeSaturationVaporPressure(T_first)

      /*
       * Compute bulk surface resistance (r_s) using equation (7) in Dhungel et al. 2014.
       */
      val r_s: RasterRDD[Float] = computeBulkSurfaceResistance(
        e_o_air, // TODO make sure this is correct
        e_a,
        ET_inst,
        r_ah
      )

      /*
       * Now that we have all the inputs, we can compute latent heat flux using the Penman-Monteith equation. We use
       * equation 8 in Dhungel et al. 2014.
       */
      val lambda_E_PM: RasterRDD[Float] = computeLatentHeatFlux(
        Delta,
        R_n,
        G,
        e_o_air,
        e_a,
        r_ah,
        r_s
      )

      /*
       * Save the output as a GeoTIFF file. We set the write mode to "compatibility" so that only a single GeoTIFF file
       * is generated.
       */
      val outputPath : String = properties.getProperty("output_path")
      lambda_E_PM.saveAsGeoTiff(
        outputPath,
        Seq(
          GeoTiffWriter.WriteMode -> "compatibility",
          GeoTiffWriter.BitsPerSample -> "8"
        )
      )

    } finally {
      spark.stop()
    }
  }
}