# DaVinci Server

This tutorial describes how to visualize data using DaVinci visualization and access it through the DaVinci server.

## Prerequisites

* Download the [ZIP code dataset](ftp://ftp2.census.gov/geo/tiger/TIGER2018/ZCTA5/tl_2018_us_zcta510.zip).
```shell
wget ftp://ftp2.census.gov/geo/tiger/TIGER2018/ZCTA5/tl_2018_us_zcta510.zip
```
* Setup Spark in local or cluster mode, below is an example command.
```shell
curl http://apache.mirrors.hoobly.com/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz | tar -xz
```
* Make the `beast` command ready on your machine [More](beast-cmd.md). The easiest method is to run the following command.
```shell
alias beast="spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.8.0-RC1 --class edu.ucr.cs.bdlab.sparkOperations.Main ."
```
## Steps

### 1. Build an index for the ZIP code data

Here, we assume that the downloaded file is in the current working directory. If your Spark is configured to read from HDFS, then you have to put it into your home directory in HDFS.

    beast index tl_2018_us_zcta510.zip iformat:shapefile \
        tl_2018_us_zcta510_index gindex:rsgrove oformat:rtree

Here is an explanation for the parameters.

| Parameter                 | Meaning             |
|---------------------------|---------------------|
|index                      |Run the index command from Beast|
|tl_2018_us_zcta510.zip     |The input file |
|iformat:shapefile          |The input file is in the shapefile format |
|tl_2018_us_zcta510_index   |The name of the generated index|
|gindex:rsgrove             |Use the R*-Grove global index|
|oformat:rtree              |The local index of the output|

After the command is finished, you might see the following log message.

    [main] INFO  edu.ucr.cs.bdlab.beast.operations.Main$  - The operation index finished in 32.781363 seconds

### 3. Build a multilevel visualization index

In this step, we use the indexed data to build a visualization index.

    beast mplot tl_2018_us_zcta510_index iformat:rtree \
       tl_2018_us_zcta510_plot \
       levels:20 plotter:gplot -no-data-tiles -mercator
    
Here is an explanation for the parameters.

| Parameter               | Meaning                     |
|-------------------------|-----------------------------|
|mplot                    | The multilevel plot command |
|tl_2018_us_zcta510_index |Use the indexed data as input|
|tl_2018_us_zcta510_plot  |The output path where the visualization data will be stored|
|iformat:rtree            |The format of the input data |
|levels:20                |Plot a total of 20 zoom levels|
|plotter:gplot            |Use the geometric plotter to plot the geometry of the data|
|-no-data-tiles           |Skip plotting data tiles, i.e., only plot image tiles|
|-mercator                |Convert the data to the web mercator projection before plotting|

You might see the following line in the log after the command is successful.

    105862 [main] INFO  edu.ucr.cs.bdlab.beast.operations.Main$  - The operation mplot finished in 104.167906 seconds

### 4. Start the visualization server

This step starts a tile server that can be used to visualize the data. Notice that the visualization command plots only image tiles which is a very small subset of all the tiles. The visualization server generates other tiles on the fly as requested.

    beast server

### 5. Explore the visualized data

Open a browser and navigate to the following URL to see the visualized data.

    http://localhost:8890/dynamic/visualize.cgi/tl_2018_us_zcta510_plot/

### 6. Download part of the data in GeoJSON format

In the browser, navigate to the following URL to download part of the data back in the compressed GeoJSON format.

    http://localhost:8890/dynamic/download.cgi/tl_2018_us_zcta510_plot.geojson?mbr=-117.515,33.85,-117.13,34.10

The downloaded data is the records bounded by the MBR (west=-117.515,south=33.85,east=-117.13,north=34.10)

You can also download the entire file back in compressed CSV format by navigating to the following URL.

    http://localhost:8890/dynamic/download.cgi/tl_2018_us_zcta510_plot.csv

## Complete example

```shell
# 0. Prerequisites
wget ftp://ftp2.census.gov/geo/tiger/TIGER2018/ZCTA5/tl_2018_us_zcta510.zip
curl http://apache.mirrors.hoobly.com/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz | tar -xz
alias beast="spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.8.0-RC1 --class edu.ucr.cs.bdlab.beast.operations.Main ."

# 2. Build an index for the ZIP code data
beast index tl_2018_us_zcta510.zip iformat:shapefile \
    tl_2018_us_zcta510_index gindex:rsgrove oformat:rtree

# 3. Build a multilevel visualization index
beast mplot tl_2018_us_zcta510_index iformat:rtree \
   tl_2018_us_zcta510_plot \
   levels:20 plotter:gplot -no-data-tiles -mercator

# 4. Start the visualization server
beast server
```

5) Explore the visualized data
In the browser, navigate to `http://localhost:8890/dynamic/visualize.cgi/tl_2018_us_zcta510_plot/`

6) Download part of the data in GeoJSON format
Navigate to `http://localhost:8890/dynamic/download.cgi/tl_2018_us_zcta510_plot.geojson?mbr=-117.515,33.85,-117.13,34.10`
    
## Common Issues

Here are a few common issues that you might face.

* After running the `index` or the `mplot` command, you see the following error.


     Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory tl_2018_us_zcta510_index already exists
     
This usually happens if you run the same command twice before cleaning up the output directory. A simple fix is to delete the output directory manually before you run the command. Another fix is to add the parameter `-overwrite` at the end of your command which will automatically delete the output directory if it already exists.

* The visualization shows the map but not the data.

This might happen if you forget to add the trailing slash `/` at the end of the URL. It is a silly mistake that we did not fix yet :)
