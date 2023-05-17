/*
 * AerodynamicResistance.scala
 * This file contains a utility method to compute bulk aerodynamic resistance.
 */

package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal

import scala.math.pow
import scala.math.log

object AerodynamicResistance {

  /*
   * Equation 13 in the 2014 paper.
   * Compute aerodynamic resistance (s/m).
   * Z_om: roughness length of momentum (m).
   * u_z: wind speed (m/s).
   * z: height of measurement (m).
   * d: zeo plane displacement (m).
   */
  def computeAerodynamicResistance(Z_om: RasterRDD[Float], u_z: RasterRDD[Float], d: Float, z: Float): RasterRDD[Float] = {
    val k: Float = 0.41f
    val Z_oh: RasterRDD[Float] = Z_om.mapPixels(x => x * 0.1f)
    val overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
      Z_om,
      Z_oh,
      u_z
    )
    val r_ah: RasterRDD[Float] = overlay.mapPixels(x => ((log((z - d) / x(0)) * log((z - d) / x(1))) / (pow(k, 2.0f).toFloat * x(3))).toFloat)
    r_ah
  }
}
