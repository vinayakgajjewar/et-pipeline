/*
 * SaturationVaporPressure.scala
 * This module contains a function to compute the saturation vapor pressure of air.
 */

package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._

import scala.math.exp

object SaturationVaporPressure {

  /*
   * Compute the saturation vapor pressure of air using equation 9 of Dhungel et al. 2014.
   * T_a:     air temperature at 30 m           (K)
   * e_o_air: saturation vapor pressure of air  (kPa)
   */
  // TODO verify units of T_a are correct.
  def computeSaturationVaporPressure(
                                    T_a: RasterRDD[Float]
                                    ): RasterRDD[Float] = {
    val e_o_air: RasterRDD[Float] = T_a.mapPixels(x => 0.611f * exp((17.27f * (x - 273.16f)) / (x - 35.86f)).toFloat)
    e_o_air
  }

}
