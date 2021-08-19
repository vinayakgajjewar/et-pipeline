/*
 * Copyright 2021 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beastExamples

import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.geolite.{IFeature, ITile}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Scala examples for Beast
 */
object RaptorExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Raptor Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    try {
      // 1- Load raster and vector data
      val treecover: RDD[ITile] = sc.geoTiff("gm_lc_v1_1_1.tif")
      val states: RDD[IFeature] = sc.shapefile("tl_2018_us_state.zip")

      // 2- Run the Raptor join operation
      val join: RDD[(IFeature, Int, Int, Int, Float)] = treecover.raptorJoin(states)
        .filter(v => v._5 >= 0 && v._5 <= 5)
      // 3- Aggregate the result
      val states_treecover: RDD[(String, Float)] = join.map(v => (v._1, v._5))
        .reduceByKey(_+_)
        .map(fv => {
          val name: String = fv._1.getAs[String]("NAME")
          val treeCover: Float = fv._2
          (name, treeCover)
        })
      // 4- Write the output
      println("State\tTreeCover")
      for (result <- states_treecover.collect())
        println(s"${result._1}\t${result._2}")
    } finally {
      spark.stop()
    }
  }
}
