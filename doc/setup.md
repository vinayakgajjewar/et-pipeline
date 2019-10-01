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

The easiest way to start is to clone the beast-examples project from BitBucket and
you might also want to base it on a stable verison of the code.

    git clone https://bitbucket.org/eldawy/beast-examples.git
    cd beast-examples
    git checkout -b mybranch 0.2.0
    

Instead, if you really want a fresh project, then you can initialize a project using Maven:

    mvn -B archetype:generate \
     -DarchetypeGroupId=org.apache.maven.archetypes \
     -DgroupId=com.mycompany.app \
     -DartifactId=my-app

Then, edit `pom.xml` and add dependency on Beast.

    <!-- https://mvnrepository.com/artifact/edu.ucr.cs.bdlab/beast -->
    <dependency>
        <groupId>edu.ucr.cs.bdlab</groupId>
        <artifactId>beast</artifactId>
        <version>0.2.0</version>
        <type>pom</type>
    </dependency>

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
