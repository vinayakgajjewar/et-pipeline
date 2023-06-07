/*
 * SensibleHeatFlux.scala
 * This module computes sensible heat flux (H) using equation (A15) of Dhungel et al. 2014.
 */

package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal

object SensibleHeatFlux {

  def computeSensibleHeatFlux(
                             T_s: RasterRDD[Float],
                             T_a: RasterRDD[Float],
                             r_ah: RasterRDD[Float]
                             ): RasterRDD[Float] = {

    /*
     * Atmospheric density (kg / m^3).
     */
    val rho_a: Float = 1.225f

    /*
     * Specific heat capacity of moist air (J/kg/K).
     */
    val C_p: Float = 1005.0f

    /*
     * Compute sensible heat flux using equation (A15) in Dhungel et al. 2014.
     */
    val overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
      T_s,
      T_a,
      r_ah
    )

    /*
     * x(0): T_s
     * x(1): T_a
     * x(2): r_ah
     */
    val H: RasterRDD[Float] = overlay.mapPixels(x => (C_p * rho_a * (x(0) - x(1))) / (x(2)))
    H
  }
}
