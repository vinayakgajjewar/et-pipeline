/*
 * SaturationVaporPressureSlope.scala
 * This file contains a utility method to calculate slope of saturation vapor pressure.
 */

package edu.ucr.cs.bdlab.beastExamples

// Import RDPro.
import edu.ucr.cs.bdlab.beast._
import scala.math.{exp, pow}

/*
 * Compute slope of saturation vapor pressure (Delta).
 */
object SaturationVaporPressureSlope {

  /*
   * Equation 11 (in the 2014 paper)
   * T_a is air temperature (K).
   */
  def computeApproxSaturationVaporPressureSlope(T_a: RasterRDD[Float]): RasterRDD[Float] = {
    return T_a.mapPixels(x => (4098.0f * (0.6108f * exp((17.27f * x) / (x + 237.3f))) / pow(x + 237.3f, 2.0f)).toFloat)
  }

}
