package dev.andymacdonald.mapreduce.mappers;

import dev.andymacdonald.mapreduce.reducers.ReduceStatistics;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class ReducerTest
{
    private ReduceDriver<Text, Text, Text, NullWritable> reduceDriver;

    @Before
    public void before()
    {
        ReduceStatistics reducer = new ReduceStatistics();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void reduce_withStandardInput_reducesToStatistics()
    {
        reduceDriver.withInput(new Text("\"Saturday Night Live\""), Arrays.asList(
                new Text("1-2-3-4"),
                new Text("1-2-3-4")));
        reduceDriver.withOutput(new Text("\"Saturday Night Live\",2,4,6,8"), null);
        reduceDriver.runTest();
    }

    @Test
    public void reduce_withInvalidTextInput_ignoresInvalidAndReturnsValid()
    {
        reduceDriver.withInput(new Text("\"Saturday Night Live\""), Arrays.asList(
                new Text("1-2-3-4"),
                new Text("fish-fish-fish-fish")));
        reduceDriver.withOutput(new Text("\"Saturday Night Live\",1,2,3,4"), null);
        reduceDriver.runTest();
    }
}