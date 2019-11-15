# `beast` command line

This tutorial explains different ways to define and use the `beast` command line for Spark.
The `beast` command is simply a short-hand for running the main class of Beast
that runs Spark operations from the command line.

## For impatient readers

Add the following line to your `~/.bashrc` and open a new shell window.

    alias beast="spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.2.0 --class edu.ucr.cs.bdlab.sparkOperations.Main ."

## Prerequisites

1. [Spark](https://spark.apache.org/): You need to have Spark installed and the command `spark-submit` available in your executable path.
It can be configured to run in the local mode or the distributed (cluster) mode.
2. [git](https://git-scm.com/): Only if you would like to clone the git repository or switch to a specific version.
3. [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html):
Only if you would like to compile Beast from source. 
3. [Maven](https://maven.apache.org/): Only if you would like to compile Beast from source.

## Method 1. No installation required

Spark allows you run a main class from any available maven library.
This is probably the easiest way to run any released version of Beast.
For example, to run beast version 0.2.0, you can define the following shorthand (alias).

    alias beast="spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.2.0 --class edu.ucr.cs.bdlab.sparkOperations.Main ."
    
After that, you can run beast by simply typing `beast`.
You can also add that line to your `~/.bashrc` or `~/.profile` to make it readily available on startup.


*Note*: If you use the above method, you will see the following error every time you run the `beast` command.

    [main] ERROR org.apache.spark.SparkContext  - Failed to add file:/home/davinci/./ to Spark environment
    java.lang.IllegalArgumentException: Directory /home/davinci/. is not allowed for addJar
 
This is normal as Spark expects a mandatory parameter for the application to run which we do not have in the case of Beast.
For now, just ignore this error.

## Method 2. Based on a JAR file

Method 1 works fine for any version that is released and available on Maven Central Repository.
However, if you have a customized version of Beast based on the source code, you need to use this second method.
First, if you do not already have it, grab your own version of Beast from the Bitbucket repository.

    git clone git@bitbucket.org:eldawy/beast.git
    
You can make any changes to the code or switch to any specific revision.
After that, compile the code to produce a runnable JAR by running the following command.

    mvn clean package -DskipTests
    
This will produce a JAR file under the `/target` directory. After that, you can define the `beast` command as follows:

    alias beast="spark-submit </full/path/to/src>/target/beast-uber-spark-*.jar"

You have to replace `</full/path/to/src>` with the absolute path to your downloaded source code to ensure the `beast`
command can run from any path.
You can also add that line to your `~/.bashrc`. Keep in mind that whenever you change the code you
will need to recompile and package the code using `mvn package` to produce the new JAR file. Moreover, if you delete
the JAR file, e.g., by running `mvn clean`, the `beast` command will stop working.