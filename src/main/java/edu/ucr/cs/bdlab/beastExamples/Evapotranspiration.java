package edu.ucr.cs.bdlab.beastExamples;

import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class Evapotranspiration {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Evapotranspiration");
        if (!conf.contains("spark.master")) {
            conf.setMaster("local[*]");
        }
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        JavaSpatialSparkContext sparkContext = new JavaSpatialSparkContext(sparkSession.sparkContext());
        try {

            // Import air temperature data
            JavaRDD<ITile<Integer>> T = sparkContext.geoTiff("data/air.sfc.2023.tif");

            // Air temperature data from NARR is given in K, let's convert it to degrees Celsius

            // Import wind speed data
            JavaRDD<ITile<Integer>> windSpeed = sparkContext.geoTiff("data/wspd.10m.mon.ltm.tif");

            // Import downward longwave radiation flux data
            JavaRDD<ITile<Integer>> downwardLongwaveRadiationFlux = sparkContext.geoTiff("data/dlwrf.2023.tif");

        } catch (Exception e) {
            System.out.println("[ERROR] There was a problem while loading raster inputs.");
            e.printStackTrace();
        } finally {
            sparkSession.stop();
        }
    }
}
