package edu.ucr.cs.bdlab.beastExamples

// Import RDPro.
import edu.ucr.cs.bdlab.beast._
import scala.math.{exp, pow}

/*
 * Compute slope of saturation vapor pressure (Delta).
 */
object SaturationVaporPressureSlope {

  /*
   * Equation 10
   * TODO if we find data to do it this way.
   */
  def compute() {}

  /*
   * Equation 11
   * T_a is air temperature (K).
   */
  def computeApproxSaturationVaporPressureSlope(T_a: RasterRDD[Float]): RasterRDD[Float] = {
    return T_a.mapPixels(x => (4098.0f * (0.6108f * exp((17.27f * x) / (x + 237.3f))) / pow(x + 237.3f, 2.0f)).toFloat)
  }

}
