package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal

/*
 * Compute bulk surface resistance (r_s) for the Penman-Monteith method.
 * Equation (7) of Dhungel et al. 2014.
 */
object BulkSurfaceResistance {

  def computeBulkSurfaceResistance(
                                    e_deg_sur: RasterRDD[Float],
                                    e_a: RasterRDD[Float],
                                    ET_inst: RasterRDD[Float],
                                    r_ah: RasterRDD[Float]
                                  ): RasterRDD[Float] = {

    /*
     * Psychrometric constant (kPa / deg. C).
     */
    val gamma: Float = 0.66f

    /*
     * Atmospheric density (kg / m^3).
     */
    val rho_a: Float = 1.225f

    /*
     * Specific heat capacity of moist air (J/kg/K).
     */
    val C_p: Float = 1005.0f

    /*
     * Compute bulk surface resistance using equation (7) of Dhungel et al. 2014.
     */
    val overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
      e_deg_sur,
      e_a,
      ET_inst,
      r_ah
    )

    /*
     * x(0): e_deg_sur
     * x(1): e_a
     * x(2): ET_inst
     * x(3): r_ah
     */
    val r_s = overlay.mapPixels(x => (((x(0) - x(1)) * C_p * rho_a) / (x(2) * gamma)) - x(3))
    r_s
  }

}
