# Add a new FeatureWriter

This article describes how to create a new FeatureWriter which writes geometric features to an output file.
In brief, you need to create a new class that extends FeatureWriter and mainly implement the write feature method.
You can find some examples in the code such as `GeoJSONFeatureWriter` and `CSVFeatureWriter`.

## 1. Create a new class


Create a new class that extends `FeatureWriter`.

```java
public class NewWriter extends FeatureWriter {}
```
## 2. Implement the `initialize` functions.

Implement two initialize functions that are called before the features are written.
This function should create the output file and prepare it for writing.
The two functions to implement are:

* `public void initialize(Path filePath, Configuration conf)`
* `public void initialize(OutputStream out, Configuration conf)`

The second method that initializes the output on a stream is not necessary to implement.
It is acceptable to throw a RuntimeException that this method is not implemented.

The simplest implementation would just create a file in the given output path and prepare it for writing.
```java
FileSystem fs = filePath.getFileSystem(conf);
this.output = fs.create(filePath);
```

## 3. Implement the `write` method

Implement (override) the method `public void write(Object dummy, IFeature f)` which should write the given feature to
the output file that has been initialized. The `dummy` parameter is not used and is usually `null`.
This method is called once for each feature in the output.
This method should *not* cache the feature for future use as it can be reused by the caller.
In other words, you should immediately use the feature and write it to the output or buffer it. 

## 4. Implement the `close` method

This method is called once to indicate that no more features will be written.
It should close the output file.
```java
public void close(TaskAttemptContext taskAttemptContext)
```

## 5. Annotate the class with `FeatureWriter.Metadata`

To allow the use of your class from the command line, you should annotate it with
the `FeatureWriter.Metadata` annotation.

```java
@FeatureWriter.Metadata(extension = ".xyz", shortName = "newwriter")
```
The extension is automatically appended to the output file created by this writer.
In other words, the parameter `filePath` passed to the `initialize` method will contain that extension.
The shortName is what the users will need to specify as a parameter to use this new writer.

# 6. Configure Beast to use the new writer

To make your new writer accessible to all components of Beast, includingn the command-line interface,
add the following lines to the file `beast.xml`
```xml
<Writers>
  <Writer>edu.ucr.cs.bdlab.beastExamples.NewWriter</Writer>
</Writers>
```
## How to use your new writer

Let's say you want to convert a file from any supported file format to your new format.
To be more specific, download the following file
[Airports](https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_airports.zip).

Then, compile your code into JAR using the following command:

```shell
mvn package
```

Let's say the generated JAR file is called `beast-example.jar`.
Now you can run the following command:
```shell
spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.9.0 \
    --class edu.ucr.cs.bdlab.beast.operations.Main beast-example.jar \
    cat ne_10m_airports.zip iformat:shapefile airports.xyz oformat:newwriter
```