# Use Beast to filter a Shapefile

This tutorial shows how to use Beast to load a Shapefile and optionally filter the loaded records based
on a rectangular range.

## Prerequisites

* Setup the development environment to use Beast. [more](setup.md)
* [Download](https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_airports.zip) a sample Shapefile for testing.
[more](https://star.cs.ucr.edu/#NE%2Fairports&center=31.41,2.03&zoom=2)

## Steps

### 1. Create a default SparkContext
```java
JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
BeastOptions opts = new BeastOptions();
```
### 2. Load features from a Shapefile

To load the features from the Shapefile as an RDD, you should use the `SpatialReader` as shown below.

```java
JavaRDD<IFeature> airports = SpatialReader.readInput(sc, opts, "ne_10m_airports.zip", "shapefile");
```
Count number of features in the file
```java
System.out.printf("Total number of airports is %d\n", airports.count());
```
### 3. Filter by range

There are two ways to filter the features from the input, either while the file is being loaded, or after the file is loaded.

#### 3a. Filter as the file is loaded

This method is usually more efficient as the filtered features (records) are never loaded from disk.
```java
opts.set(SpatialInputFormat.FilterMBR, "-128.1,27.3,-63.8,54.3");
``` 
The format of the MBR is `x_min,y_min,x_max,y_max` or for geographical data it is `west,south,east,north`.

After that, you load the file using the regular method but pass the updated user options.
```java
JavaRDD<IFeature> filtered_airports = SpatialReader.readInput(sc, opts, "ne_10m_airports.zip", "shapefile");
System.out.printf("Number of loaded airports is %d\n", filtered_airports.count());
```    
#### 3b. Filter after the records are loaded

Another method is to first load all the records and then filter them using the RDD filter method as follows.

```java
Envelope range = new Envelope(2, -128.1, 27.3, -63.8, 54.3);
JavaRDD<IFeature> filtered_airports2 = airports.filter(f -> range.contains(f.getGeometry()));
System.out.printf("Number of filtered airports is %d\n", filtered_airports2.count());
```
While this method can be less efficient as it first loads the records and then filters them,
it is more powerful as you can use any spatial range, e.g., an arbitrary polygon, and any spatial predicate,
e.g., contains.

## Complete example

Below is the
[complete code](https://bitbucket.org/eldawy/beast-examples/src/master/src/main/java/edu/ucr/cs/bdlab/beastExamples/FilterFeatures.java)
that you can run.
```java
 package edu.ucr.cs.bdlab.beastExamples;
 
 import edu.ucr.cs.bdlab.geolite.Envelope;
 import edu.ucr.cs.bdlab.geolite.IFeature;
 import edu.ucr.cs.bdlab.io.SpatialInputFormat;
 import edu.ucr.cs.bdlab.sparkOperations.SpatialReader;
 import edu.ucr.cs.bdlab.util.UserOptions;
 import org.apache.spark.api.java.JavaRDD;
 import org.apache.spark.api.java.JavaSparkContext;
 
 public class FilterFeatures {
   public static void main(String[] args) {
     // 1. Create a default SparkContext
     try (JavaSparkContext sc = new JavaSparkContext("local[*]", "test")) {
       BeastOptions opts = new BeastOptions();
       // 2. Load features from a Shapefile
       JavaRDD<IFeature> airports = SpatialReader.readInput(sc, opts, "ne_10m_airports.zip", "shapefile");
       System.out.printf("Total number of airports is %d\n", airports.count());
 
       // 3a. Filter as the file is loaded
       opts.set(SpatialInputFormat.FilterMBR, "-128.1,27.3,-63.8,54.3");
       JavaRDD<IFeature> filtered_airports = SpatialReader.readInput(sc, opts, "ne_10m_airports.zip", "shapefile");
       System.out.printf("Number of loaded airports is %d\n", filtered_airports.count());
 
       // 3b. Filter after the records are loaded
       Envelope range = new Envelope(2, -128.1, 27.3, -63.8, 54.3);
       JavaRDD<IFeature> filtered_airports2 = airports.filter(f -> range.contains(f.getGeometry()));
       System.out.printf("Number of filtered airports is %d\n", filtered_airports2.count());
     }
   }
 }
```
## Run the example

The simplest way to run this example is from your favorite IDE, e.g., [IntelliJ IDEA](https://www.jetbrains.com/idea/)
or Eclipse, as a regular Java program.
Alternatively, you can run it from the command line as follows.

Package your project into JAR
```shell
mvn package
```

Run the JAR using `spark-submit` as shown below assuming that the generated JAR file is named `beast-examples-0.2.0.jar`

```shell
spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.9.0 \
    --class edu.ucr.cs.bdlab.beastExamples.FilterFeatures target/beast-examples-0.9.0.jar
```