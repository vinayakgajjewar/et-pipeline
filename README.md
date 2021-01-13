# Beast Examples

This repository provides a few examples to use [Beast](https://www.bitbucket.org/eldawy/beast).
To compile the code and generate a JAR file, run the command `mvn package`.
This command generates two JAR files in the `target` subdirectory named,
`beast-examples-<version>.jar` and `beast-uber-examples-<version>.jar`.
To run any of the examples, using one of the following commands:

```shell
spark-submit beast-uber-examples-<version>.jar <command>
```

If you have Beast installed, you can run the project using the following command:
```shell
beast --jars beast-examples-<version>.jar <command>
```

## Main function
You can write your code in a standalone main class such as this
[Java example](src/main/java/edu/ucr/cs/bdlab/beastExamples/JavaExamples.java)
or [Scala example](src/main/scala/edu/ucr/cs/bdlab/beastExamples/ScalaExamples.scala).
To run one of these examples, you can use the following command.
```shell
beast --jars beast-examples-<version>.jar --class edu.ucr.cs.bdlab.beastExamples.ScalaExamples
```

## Operations
The code contains examples of new operations in Spark, e.g., IndexVisualization and PointInPolygon. To add a new operation, create a class
and annotate it with `OperationMetadata`. This class has to contain at least a `run` operation.
In addition, to add the operation to the command line interface (CLI), you need to add
the full class name in the file `resources/beast.xml`.

For example, to run the operation
[PointInPolygon](src/main/java/edu/ucr/cs/bdlab/beastExamples/PointInPolygon.java),
you can use the following command:
```shell
beast --jars beast-examples-<version>.jar pip points.shp polygons.shp output
```

## Input file formats
You can also add a new input file format by creating a class that implements the interface
`FeatureReader`. To access the new input format from command line, you need to add the full class name
to the file `resources/beast.xml`.

For example, to use the
[RoadsReader](src/main/java/edu/ucr/cs/bdlab/beastExamples/RoadsReader.java) reader,
use the following command:
```shell
beast --jars beast-examples-<version>.jar summary roads.csv iformat:roadnetwork
```

## Plotters
To add a new plotter, create a new class that extends the `Plotter` interface and annotate it with
the `Plotter.Metadata` annotation. To make it accessible from command line, you must add the full
class name to `resources/beast.xml` under section &lt;Plotters&gt;.

To use the
[ClusterPlotter](src/main/java/edu/ucr/cs/bdlab/beastExamples/ClusterPlotter.java),
you can use a command like the following:
```shell
beast --jars beast-examples-<version>.jar splot tweets.shp iformat:shapefile tweets.png plotter:cgplot
```