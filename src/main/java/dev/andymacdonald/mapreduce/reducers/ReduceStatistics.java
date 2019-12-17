package dev.andymacdonald.mapreduce.reducers;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static java.lang.String.format;

public final class ReduceStatistics extends Reducer<Text, Text, Text, NullWritable>
{
    private static final String HYPHEN_DELIMITER = "-";
    public static final int VIEW_INDEX = 0;
    public static final int LIKES_INDEX = 1;
    public static final int DISLIKES_INDEX = 2;
    public static final int COMMENT_COUNT_INDEX = 3;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        long viewsSum = 0L;
        long likesSum = 0L;
        long dislikesSum = 0L;
        long commentSum = 0L;
        for (Text x : values)
        {
            String value = x.toString();
            String[] splitScores = value.split(HYPHEN_DELIMITER);
            try
            {
                long viewEntry = Long.parseLong(splitScores[VIEW_INDEX]);
                long likesEntry = Long.parseLong(splitScores[LIKES_INDEX]);
                long dislikesEntry = Long.parseLong(splitScores[DISLIKES_INDEX]);
                long commentCountEntry = Long.parseLong(splitScores[COMMENT_COUNT_INDEX]);
                viewsSum += viewEntry;
                likesSum += likesEntry;
                dislikesSum += dislikesEntry;
                commentSum += commentCountEntry;
            }
            catch (NumberFormatException ex)
            {
                // Do nothing
            }
        }
        context.write(new Text(format("%s,%s,%s,%s,%s",key, viewsSum, likesSum, dislikesSum, commentSum)), null);
    }
}
