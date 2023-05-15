/*
 * AerodynamicResistance.scala
 * This file contains a utility method to compute bulk aerodynamic resistance.
 */

package edu.ucr.cs.bdlab.beastExamples

// Import RDPro.
import edu.ucr.cs.bdlab.beast._

object AerodynamicResistance {

  /*
   * Equation 13 in the 2014 paper.
   * Compute aerodynamic resistance (s/m).
   */
  def computeAerodynamicResistance(Z_om: RasterRDD[Float], u_z: RasterRDD[Float], d: ??): Unit = {
    val k: Float = 0.41f
  }
}
