function clear_exiting_local_outputs() {
    rm -rf /local-output/*
}

function clear_hdfs_directories() {
  hadoop fs -rm -r /input
  hadoop fs -mkdir -p /input
  hadoop fs -rm -r /output
}

function run_hadoop_mapreduce_job() {
    JAR_NAME=$1
    CLASS_NAME=$2
    hadoop jar ${JAR_NAME} ${CLASS_NAME}  /input /output
}

function copy_input_from_local() {
    hadoop fs -copyFromLocal /local-input/* /input/
}

function copy_output_to_local() {
    hadoop fs -copyToLocal /output/* /local-output
}

cd $HADOOP_PREFIX
echo "Waiting for HDFS startup"
hdfs dfsadmin -safemode leave
clear_hdfs_directories
clear_exiting_local_outputs
copy_input_from_local

run_hadoop_mapreduce_job /opt/mapreduce/MapReduceDataset.jar dev.andymacdonald.mapreduce.MapReduceDataset

copy_output_to_local
