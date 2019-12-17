# Hadoop MapReduce Dataset Example #

An example Hadoop setup using Docker-Compose and MapReduce job that generates descriptive statistics from the [Trending Youtube Video Statistics](https://www.kaggle.com/datasnaek/youtube-new) dataset.

## Requirements ##

* `Docker-Compose`
* `Docker`

## Run Hadoop Cluster

* `make hadoop-cluster`

## Hadoop MapReduce Job Example ##

* Download and extract dataset to temporary location.
* Move *.csv files into `input/` directory
* `make run-map-reduce`

#### Example Job Output

The job will sum channel statistics (`views, likes, dislikes and comment count`) for the above dataset by channel name:

**Example**: `part-r-00000`
```csv
"Peter Nicholls",702840,10503,7078,4229
"Peter Parker Tube",980538,628,1435,840
"PewDiePie",1081580672,68390145,4100674,9560538
"Pina Records",2098254446,19296587,1530702,829335
"PinkVEVO",441626351,8320768,330944,494822
```