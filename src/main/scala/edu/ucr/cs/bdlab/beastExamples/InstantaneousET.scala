package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.raptor.RasterOperationsLocal

import scala.math.pow

/*
 * Compute instantaneous evapotranspiration using equations (1) and (52) of Allen et al. 2007.
 */
object InstantaneousET {

  def computeInstantaneousET(
                            T_s: RasterRDD[Float],
                            R_n: RasterRDD[Float],
                            G: RasterRDD[Float],
                            H: RasterRDD[Float]
                            ): RasterRDD[Float] = {

    /*
     * Compute latent heat of vaporization using equation (53) of Allen et al. 2007.
     */
    val lambda: RasterRDD[Float] = T_s.mapPixels(x => (2.501f - 0.00236f * (x - 273.15f)) * pow(10, 6).toFloat)

    /*
     * Compute instantaneous ET using equations (1) and (52) of Allen et al. 2007.
     */
    val ET_inst_overlay: RasterRDD[Array[Float]] = RasterOperationsLocal.overlay(
      R_n,
      G,
      H,
      lambda
    )

    /*
     * x(0): R_n
     * x(1): G
     * x(2): H
     * x(3): lambda
     */
    val ET_inst: RasterRDD[Float] = ET_inst_overlay.mapPixels(x => 3600 * (x(0) - x(1) - x(2)) / (x(3) * 1000))
    ET_inst
  }
}
