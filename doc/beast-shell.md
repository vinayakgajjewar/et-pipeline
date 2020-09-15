# `Beast-shell` Scala shell

This tutorial explains how to access Beast functions in the Spark Scala shell.

## For impatient readers

Add the following line to your `~/.bashrc` and open a new shell window.

```shell
alias beast-shell="spark-shell --repositories https://repo.osgeo.org/repository/release/ --packages edu.ucr.cs.bdlab:beast-spark:0.7.0 --exclude-packages javax.media:jai_core"
```

## Prerequisites

1. [Spark](https://spark.apache.org/): You need to have Spark installed and the command `spark-shell` available in your executable path.
It can be configured to run in the local mode or the distributed (cluster) mode.

## Method 1: Add the `beast-shell` alias

This method runs the normal `spark-shell` and additionally load Beast packages.
This approach can be useful to use one of the release versions of Beast that are available on Maven.
 
```shell
alias beast-shell="spark-shell --repositories https://repo.osgeo.org/repository/release/ --packages edu.ucr.cs.bdlab:beast-spark:0.7.0 --exclude-packages javax.media:jai_core"
```

In the command line, type `beast-shell` and let it start. To access all Beast Scala shortcuts,
run the following command first inside the shell.

```scala
import edu.ucr.cs.bdlab.beast._
```

## Method 2: Use a Beast JAR file

If you have a packaged JAR file that contains Beast and all its dependencies, i.e., an uber JAR,
you can use it with the Spark shell using the following command.

```shell
spark-shell --jars beast-uber-spark-0.7.1-SNAPSHOT.jar
```
Again, you should start with the following import to access the Scala shortcuts.
```scala
import edu.ucr.cs.bdlab.beast._
```

## Automatically import Beast
If you use the Beast shell frequently, you might want to automatically import
the Scala shortcuts as part. You can do so by following the steps below.

1. Create a Scala file in your home directory named `beast-init.scala`
with the import line as shown below.

```shell
echo "import edu.ucr.cs.bdlab.beast._" > $HOME/beast-init.scala
```

2. Edit your `beast-shell` alias to look like the following.

```shell
alias beast-shell="spark-shell --repositories https://repo.osgeo.org/repository/release/ --packages edu.ucr.cs.bdlab:beast-spark:0.7.0 --exclude-packages javax.media:jai_core -I $HOME/beast-init.scala"
```

Now, to start the Beast shell, just type `beast-shell` and it will automatically load the Scala shortcuts.