# MODIS Data Download

This tutorial describes how to automate the download of MODIS files for use in your experiments.

## Prerequisites

* Ruby interpreter [Download here](https://www.ruby-lang.org/en/).
* An active account with NASA Earth Data [Create one here](https://urs.earthdata.nasa.gov/users/new).

## Steps

### 1. Locate the dataset

We use the LP DAAC data pool to download the data directly.
Each satellite product has a unique short name that is used to identify it in the pool.
You can find the full list on [this page](https://lpdaac.usgs.gov/product_search/?collections=Combined+MODIS&collections=Terra+MODIS&collections=Aqua+MODIS&status=Operational&view=list).

Let us say that you are interested in temperature data (short name = [MYD11A1.006](https://lpdaac.usgs.gov/products/myd11a1v006/)),
You can locate it in the data pool on [this link](https://e4ftl01.cr.usgs.gov/MOLA/MYD11A1.006/).
If you choose another dataset, you can start at the [root of the data pool](https://lpdaac.usgs.gov/tools/data-pool/) and navigate
your way based on the code name and the type of the dataset.
All what we need in this step, is the URL of the dataset you want to download, i.e., `https://e4ftl01.cr.usgs.gov/MOLA/MYD11A1.006/`.

### 2. Setup your login credentials

While you can browse the LP DAAC archive anonymously, you need an active account to download the files.
This step sets up your credentials to use with the download script.
Simply, create a file named `~/.netrc` with the following format.

```text
machine urs.earthdata.nasa.gov
        login <username>
        password <userpassword>
```
Replace `<username>` and `<password>` with your username and password that you used
to create the account.
For more information, check the [detailed instructions](https://lpdaac.usgs.gov/resources/e-learning/how-access-lp-daac-data-command-line/).

### 3. Download the download script

The download script is located as part of the [Raptor source code](https://bitbucket.org/eldawy/beast/src/master/raptor/).
The file is located on [this link](https://bitbucket.org/eldawy/beast/raw/88fcf2248e28bf7b722aee4ba339d3462864992e/raptor/src/main/ruby/hdf_downloader.rb).
Simple download the file or copy and paste the script into a local `hdf_downloader.rb` file.

### 4. Locate the geographical region and date interval

While this script can be used to download the entire directory, you do not want to do that
as it might be several terabytes of data. Rather, you want to limit the download to a specific
geographical region and time interval. The easiest way is to use
[OpenStreetMap export tool](https://www.openstreetmap.org/export#map=7/36.743/-120.536)
which shows the boundaries of the map as you move around.
Keep note of the four numbers west, south, east, and north.
For example, to download the data of California, the range format is
`-125.530,30.751,-112.742,43.213`

The date interval is in the format `yyyy.mm.dd..yyyy.mm.dd`.
For example, to download all the data from January 1st, 2018 to
January 7th, 2018, the date interval is `2018.01.01..2018.01.07`.

### 5. Run the script

The following command will launch the download script to download the temperature
files of California from January 1st, 2018 to January 7th, 2018.

    ruby hdf_downloader.rb https://e4ftl01.cr.usgs.gov/MOLA/MYD11A1.006/ rect:-125.530,30.751,-112.742,43.213 time:2018.01.01..2018.01.07
    
The total download size for the above call is around 100 MB.

## Notes
If some of the download files fail, you can rerun the same command to download only the remaining files.
The script automatically checks if a file with the same name already exists in your local directory and
skips the download of these files.

If you decide to download more files by expanding the region or the time interval,
the script will avoid redownloading the existing files which have been
already downloaded.