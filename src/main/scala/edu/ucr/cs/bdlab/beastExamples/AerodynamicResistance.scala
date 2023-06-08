/*
 * AerodynamicResistance.scala
 * This file contains a utility method to compute bulk aerodynamic resistance.
 */

package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._

import scala.math.pow
import scala.math.log

object AerodynamicResistance {

  /*
   * Equation 13 in the 2014 paper.
   * Compute aerodynamic resistance (s/m).
   * Z_om: roughness length of momentum (m).
   * u_z: wind speed (m/s).
   * z: height of measurement (m).
   * d: zero plane displacement (m).
   */
  def computeAerodynamicResistance(
                                    Z_om: Float,
                                    u_z: RasterRDD[Float],
                                    d: Float,
                                    z: Float
                                  ): RasterRDD[Float] = {
    val k: Float = 0.41f

    /*
     * Z_oh can be approximated as 0.1 * Z_om.
     */
    val Z_oh: Float = Z_om * 0.1f

    val r_ah: RasterRDD[Float] = u_z.mapPixels(x => (log((z - d) / Z_om).toFloat * log((z - d) / Z_oh).toFloat) / (pow(k, 2).toFloat * x))
    r_ah
  }
}
