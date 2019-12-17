package dev.andymacdonald.mapreduce;

import dev.andymacdonald.mapreduce.mappers.MapStatistics;
import dev.andymacdonald.mapreduce.reducers.ReduceStatistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceDataset
{
    public static void main(String[] args) throws Exception
    {
        Job job = Job.getInstance();

        job.setJarByClass(MapReduceDataset.class);

        job.setJobName("Trending Statistics");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setMapperClass(MapStatistics.class);
        job.setReducerClass(ReduceStatistics.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }


}
