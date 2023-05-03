// Pipeline.scala

// Units and variables:
// https://www.emc.ncep.noaa.gov/mmb/rreanl/narr_archive_contents.pdf

// Data source:
// https://psl.noaa.gov/data/gridded/data.narr.html

package edu.ucr.cs.bdlab.beastExamples

// RDPro
import edu.ucr.cs.bdlab.beast._

import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.raptor.{RasterOperationsFocal, RasterOperationsLocal}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.math.exp
import scala.math.pow
import scala.math.log

object Pipeline {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val conf = new SparkConf().setAppName("Evapotranspiration Pipeline")
    conf.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    try {

      // Raster metadata for reshaping
      // TODO what values?
      val metadata = RasterMetadata.create(x1 = -50, y1 = 40, x2 = -60, y2 = 30, srid = 4326, rasterWidth = 10, rasterHeight = 10, tileWidth = 10, tileHeight = 10)

      // Load air temp data
      // TODO: let's not hardcode file paths, maybe a config file?
      var T = sc.geoTiff[Float]("file:///Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/data/air.sfc.2023.tif")
      T = RasterOperationsFocal.reshapeNN(T, metadata)
      //val T_flat = T.flatten

      //T_flat.map(_._4(0) - 272.15f)

      // Air temp data from NARR is in K, so let's convert to degrees Celsius
      //T = T.mapPixels(x => x(0) - 272.15f)
      val T_min = T.flatten.map(_._4).min()
      val T_max = T.flatten.map(_._4).max()

      // Load relative humidity data
      // Relative humidity data given as %
      val RH = sc.geoTiff[Float]("file:///Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/data/rhum.2m.2023.tif")
      val RH_max = RH.flatten.map(_._4).max()
      val RH_min = RH.flatten.map(_._4).min()

      // Load wind speed data
      // Units: m s^-1
      // 10 m above sea level
      val u_z = sc.geoTiff[Float]("file:///Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/data/wspd.10m.mon.ltm.tif")

      // Load downward shortwave radiation flux data
      val R_s = sc.geoTiff[Float]("file:///Users/vinayakgajjewar/Fall_2022_research/evapotranspiration-pipeline/beast-examples/data/dswrf.2023.tif")

      // Equation 7
      // Compute atmospheric pressure (P) from elevation (z)
      // P is in kPa, z is in meters
      val z = 1234 // TODO we need actual data for z
      val P = 101.3 * pow((293 - 0.0065 * z) / 293, 5.26)

      // Equation 8
      // Compute psychometric constant (gamma) from atmospheric pressure
      // kPa / deg C
      val gamma = 0.665 * pow(10, -3) * P

      // Equation 11
      // We are calculating saturation vapor pressure here
      // We need to calculate for both min and max temps
      val e_T_min = 0.6108 * exp((17.27 * T_min) / (T_min + 237.3))
      val e_T_max = 0.6108 * exp((17.27 * T_max) / (T_max + 237.3))

      // Equation 12
      // Calculate mean saturation vapor pressure
      val e_s = (e_T_max + e_T_min) / 2

      // Equation 13
      // Here, we calculate Delta (slope of saturation vapor pressure curve)
      val Delta = T.mapPixels(x => 4098 * (0.6108 * exp((17.27 * x) / (x + 237.3))) / pow(x + 237.3, 2))

      // test
      // I don't know why we have to cast to RDD[ITile[AnyVal]], but it doesn't work if we don't cast it
      //val test = RasterOperationsLocal.overlay(Delta.asInstanceOf[RDD[ITile[AnyVal]]], T.asInstanceOf[RDD[ITile[AnyVal]]]).flatten.map(x => x._1 + x._2)

      // Equation 17
      // Here, we get actual vapor pressure (e_a) from relative humidity data
      // We are assuming that we have both RH_max and RH_min (as %)
      val e_a = (e_T_min * RH_max / 2 + e_T_max * RH_min / 2) / 2

      // Equation 38
      // Compute net shortwave radiation from incoming solar radiation
      // Canopy reflection coefficient is defined as 23% for hypothetical grass reference crop
      val a = 0.23
      val R_ns = R_s.mapPixels(x => (1 - a) * x)

      // Equation 39

      // Equation 40
      // Compute net radiation (R_n) from incoming net shortwave radiation (R_ns) and outgoing net longwave radiation (R_nl)
      // TODO
      //val R_n = R_ns - R_nl

      // Compute soil heat flux

      // For single-day and ten-day periods, soil heat flux is negligible
      // Equation 42
      val G = 0

      // Equation 47
      // Compute wind speed at 2 meters given wind speed at height z
      // meters/second
      // TODO: temporarily hardcoding value of z
      val u_2 = u_z.mapPixels(x => 4.87 / log(67.8 * 10 - 5.42))

      // Equation 6
      // FAO Penman-Monteith
      // TODO convert this to code
      val ET_o = 69420


    } finally {
      spark.stop()
    }
  }
}
