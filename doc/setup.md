# Setup the development environment for Beast

This page describes how to use Beast in your development environment.
This can help you, for example, create new visualizers or new input/output formats.

## Prerequisites

In order to use Beast, you need the following prerequistes installed on your machine.

* Java Development Kit (JDK). [Oracle JDK](https://www.oracle.com/technetwork/java/javase/downloads/index.html) 1.8 or later is recommended.
* [Apache Maven](https://maven.apache.org/)
* [Git](https://git-scm.com/)
* [Spark](https://spark.apache.org). Standalone mode is fine.

## Create a project

If you have an existing Maven-based project, then you can integrate it with Beast by
adding the following dependency to your `pom.xml` file.

    <!-- https://mvnrepository.com/artifact/edu.ucr.cs.bdlab/beast -->
    <dependency>
      <groupId>edu.ucr.cs.bdlab</groupId>
      <artifactId>beast-spark</artifactId>
      <version>0.2.0</version>
    </dependency>

Instead, you can [first create a new Maven project](https://maven.apache.org/guides/getting-started/index.html#How_do_I_make_my_first_Maven_project)
before adding the Beast dependency.

Another option is to clone the beast-examples project from BitBucket and
you might also want to base it on a stable version of the code.

    git clone https://bitbucket.org/eldawy/beast-examples.git
    cd beast-examples
    git checkout -b mybranch 0.2.0

## Write your code

Now, you can write your code in the new project.

## Package

To package your code into JAR, simple run the following command.

    mvn package

This will generate a new JAR under `target/` directory.

# Run

If your JAR does not contain a main file or if you want to run one of the standard
operations in Beast, use the following command.

    spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.2.0 \
       --jars target/my-app.jar \
       edu.ucr.cs.bdlab.sparkOperations.Main

Instead, if your project contains a main class that you want to run, you should run the following command:

    spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.2.0 \
       --jars target/my-app.jar \
       <class name>
