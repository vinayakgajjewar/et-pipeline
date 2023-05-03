package edu.ucr.cs.bdlab.beastExamples;

// BILReader.java
// Tested using jdk-15.0.2
// Based on the following code:
// http://gis.ess.washington.edu/projects/java/Bil.java
// TODO: this program currently assumes only 1 band, we need to extend it to multiple

import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.File;

import java.util.*;

public class BILReader {

    // TODO: convert these fields to a key-value data structure
    private int numRows;
    private int numCols;
    private int numBands;
    private int numBits;
    private int noDataVal;

    // Data array
    private int[][] data;

    // Getter methods

    public int getNumRows() {
        return this.numRows;
    }

    public int getNumCols() {
        return this.numCols;
    }

    public int getNumBands() {
        return this.numBands;
    }

    public int getNumBits() {
        return this.numBits;
    }

    public int getNoDataVal() {
        return this.noDataVal;
    }

    // Constructor
    BILReader(String root) throws IOException {

        List<String> fields = Arrays.asList("NROWS", "NCOLS", "NBANDS", "NBITS", "NODATA");

        // First, look for a header file
        String headerFile = root + ".hdr";
        System.out.println("Looking for header file: " + headerFile);
        StreamTokenizer headerStreamTokenizer = new StreamTokenizer(new BufferedReader(new InputStreamReader(new FileInputStream(headerFile))));

        // Print out all the tokens in the header file
        while (true) {
            int tokenType = headerStreamTokenizer.nextToken();
            if (tokenType == StreamTokenizer.TT_WORD) {
                String word = headerStreamTokenizer.sval;

                // We're only interested in these fields
                if (fields.contains(word)) {
                    System.out.print(word + ":\t");
                    tokenType = headerStreamTokenizer.nextToken();

                    // Throw an error if the value is not a number
                    if (tokenType != StreamTokenizer.TT_NUMBER) {
                        throw new IOException("Error: expected number while reading " + word + " field");
                    }

                    int val = (int) headerStreamTokenizer.nval;
                    System.out.println(Integer.toString(val));

                    // Read the value
                    switch (word) {
                        case "NROWS":
                            this.numRows = val;
                            break;
                        case "NCOLS":
                            this.numCols = val;
                            break;
                        case "NBANDS":
                            this.numBands = val;
                            break;
                        case "NBITS":
                            this.numBits = val;
                            break;
                        case "NODATA":
                            this.noDataVal = val;
                            break;
                    }
                }
            } else if (tokenType == StreamTokenizer.TT_EOF) {
                // End of file reached
                System.out.println("EOF reached");
                break;
            }
        }

        // PRISM climate data is 32-bit so we only need to worry about that for now
        // Storing the data in an integer array will suffice
        // TODO: we should be able to handle other BIL files too eventually
        if (this.numBits > 32) {
            throw new IOException("Error: this program currently only supports 32-bit files");
        }

        // Now that we've read the header file, we can read the actual BIL file
        String bilFile = root + ".bil";
        System.out.println("Looking for BIL file: " + bilFile);

        // Read BIL file
        DataInputStream bilDataInput = new DataInputStream(new FileInputStream(new File(bilFile)));

        // Initialize data array
        data = new int[this.numRows][this.numCols];

        // Populate data array
        for(int i = 0; i < numRows; i++){
            for(int j = 0; j < numCols; j++){
                this.data[i][j] = bilDataInput.readInt();
                //System.out.print(dataArr[i][j] + ",");
            }
            //System.out.println();
        }

        System.out.println("Successfully read BIL file");
    }
    public static void main(String[] args) {
        try {
            BILReader bilReader = new BILReader("PRISM_ppt_stable_4kmM3_202101_bil/PRISM_ppt_stable_4kmM3_202101_bil");
            System.out.println("Rows:\t" + Integer.toString(bilReader.getNumRows()));
            System.out.println("Cols:\t" + Integer.toString(bilReader.getNumCols()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkConf conf = new SparkConf().setAppName("Evapotranspiration");
        if (!conf.contains("spark.master")) {
            conf.setMaster("local[*]");
        }
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        JavaSpatialSparkContext sparkContext = new JavaSpatialSparkContext(sparkSession.sparkContext());
        try {
            RasterMetadata metadata = RasterMetadata.create(-125,49.916666666667,-66.45834,24.0624,4269,1405,621,10,10);
            JavaRDD<Integer> pixels = sparkContext.parallelize(Arrays.asList(3, 6, 3, 2));
        } finally {
            sparkSession.stop();
        }
    }
}
