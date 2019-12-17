package dev.andymacdonald.mapreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static java.lang.String.format;

public final class MapStatistics extends Mapper<LongWritable, Text, Text, Text>
{
    private static final String COMMA_DELIMITER = ",";
    public static final int CHANNEL_ID_INDEX = 3;
    public static final int VIEW_INDEX = 7;
    public static final int LIKES_INDEX = 8;
    public static final int DISLIKES_INDEX = 9;
    public static final int COMMENT_COUNT_INDEX = 10;
    private Text channelIdViewsLikesDislikesCommentCount = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        List<String> csvRecordsOfLine = getRecordsFromLine(value.toString());
        try
        {
            String channelId = csvRecordsOfLine.get(CHANNEL_ID_INDEX);
            String views = csvRecordsOfLine.get(VIEW_INDEX);
            String likes = csvRecordsOfLine.get(LIKES_INDEX);
            String dislikes = csvRecordsOfLine.get(DISLIKES_INDEX);
            String commentCount = csvRecordsOfLine.get(COMMENT_COUNT_INDEX);
            channelIdViewsLikesDislikesCommentCount.set(format("%s-%s-%s-%s", views, likes, dislikes, commentCount));
            context.write(new Text(channelId), channelIdViewsLikesDislikesCommentCount);
        }
        catch (IndexOutOfBoundsException ex)
        {
            // Do nothing
        }
    }

    private List<String> getRecordsFromLine(String line)
    {
        List<String> values = new ArrayList<>();
        try (Scanner rowScanner = new Scanner(line))
        {
            rowScanner.useDelimiter(COMMA_DELIMITER);
            while (rowScanner.hasNext())
            {
                values.add(rowScanner.next());
            }
        }
        return values;
    }
}
