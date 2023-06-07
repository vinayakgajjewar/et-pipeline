/*
 * PenmanMonteith.scala
 * This object contains a utility method to compute latent heat flux using the Penman-Monteith method.
 */

package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal

object PenmanMonteith {

  /*
   * r_s:   bulk surface resistance (s/m)
   * gamma: psychrometric constant  (kPa C^-1)
   */
  def computeLatentHeatFlux(
                           Delta: RasterRDD[Float],
                           R_n: RasterRDD[Float],
                           G: RasterRDD[Float],
                           e_o_air: RasterRDD[Float],
                           e_a: RasterRDD[Float],
                           r_ah: RasterRDD[Float],
                           gamma: RasterRDD[Float],
                           r_s: RasterRDD[Float]
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
     * Create an overlay with all the input rasters.
     */
    val overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
      Delta,
      R_n,
      G,
      e_o_air,
      e_a,
      C_p,
      rho_a,
      r_ah,
      gamma,
      r_s
    )

    /*
     * Equation 8 in Dhungel et al. 2014.
     * x(0): Delta
     * x(1): R_n
     * x(2): G
     * x(3): e_o_air
     * x(4): e_a
     * x(5): r_ah
     * x(6): gamma
     * x(7): r_s
     */
    val lambda_E_PM: RasterRDD[Float] = overlay.mapPixels(x => ((x(0) * (x(1) - x(2))) + ((C_p * rho_a * (x(3) - x(4))) / x(5))) / (x(0) + x(6) * (1 + (x(7) / x(5)))))
    lambda_E_PM
  }
}
