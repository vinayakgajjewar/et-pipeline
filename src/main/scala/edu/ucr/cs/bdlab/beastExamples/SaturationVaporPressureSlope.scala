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
   * This computation is an approximation that doesn't use surface temperature (which isn't commonly measured).
   */
  def computeApproxSaturationVaporPressureSlope(T_a: RasterRDD[Float]): RasterRDD[Float] = {

    /*
     * First we convert T_a from Kelvin to degrees Celsius.
     */
    val T_a_deg_C = T_a.mapPixels(x => x - 273.15)

    /*
     * Now we can compute Delta using equation 11 from Dhungel et al. 2014.
     */
    return T_a_deg_C.mapPixels(x => (4098.0f * (0.6108f * exp((17.27f * x) / (x + 237.3f))) / pow(x + 237.3f, 2.0f)).toFloat)
  }

}
