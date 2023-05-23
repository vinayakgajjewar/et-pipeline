/*
 * NetRadiation.scala
 * This file contains a utility method to compute net radiation via equation 6 of Dhungel et al. 2014.
 */

package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal

object NetRadiation {

  /*
   * R_sd:  downward shortwave radiation  (W m^-1)
   * R_ld:  downward longwave radiation   (W m^-1)
   * R_lu:  upward longwave radiation     (W m^-1)
   * alpha: surface albedo
   */
  def computeNetRadiation(
                           R_sd: RasterRDD[Float],
                           R_ld: RasterRDD[Float],
                           R_lu: RasterRDD[Float],
                           alpha: RasterRDD[Float]
                         ): RasterRDD[Float] = {
    val overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
      R_sd,
      R_ld,
      R_lu,
      alpha
    )

    /*
     * x(0): R_sd
     * x(1): R_ld
     * x(2): R_lu
     * x(3): alpha
     * TODO I need to compute broadband emissivity properly using eq. 17 of the METRIC 2007 paper.
     */
    val R_n: RasterRDD[Float] = overlay.mapPixels(x => (1 - x(3)) * x(0) + x(1) - x(2) - (1 - 0.98f) * x(1))
    R_n
  }
}
