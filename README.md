Beast Examples
==============

This repository provides a few examples to use [Beast](https://www.bitbucket.org/eldawy/beast).
To compile the code and generate a JAR file, run the command `mvn package`.
This command generates two JAR files in the `target` subdirectory named,
`beast-examples-<version>.jar` and `beast-uber-examples-<version>.jar`.
To run any of the examples, using one of the following commands:

    spark-submit beast-uber-examples-<version>.jar

If you need to run it with a specific version of Beast, e.g., a custom version, use the
following command: 

    spark-submit --jars beast-examples-<version>.jar beast-uber-spark-<version>.jar
    
Operations
----------
The code contains examples of new operations in Spark, e.g., IndexVisualization and PointInPolygon. To add a new operation, create a class
and annotate it with `OperationMetadata`. This class has to contain at least a `run` operation.
In addition, to add the operation to the command line interface (CLI), you need to add
the full class name in the file `resources/beast.xml`.

Input file formats
------------------
You can also add a new input file format by creating a class that implements the interface
`FeatureReader`. To access the new input format from command line, you need to add the full class name
to the file `resources/beast.xml`.

Plotters
--------
To add a new plotter, create a new class that extends the `Plotter` interface and annotate it with
the `Plotter.Metadata` annotation. To make it accessible from command line, you must add the full
class name to `resources/beast.xml` under section &lt;Plotters&gt;.