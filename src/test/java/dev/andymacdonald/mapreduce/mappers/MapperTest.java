package dev.andymacdonald.mapreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class MapperTest
{
    private MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void before()
    {
        MapStatistics mapper = new MapStatistics();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void map_withStandardExampleLine_writesAppropriateMappedEntryToContext()
    {
        mapDriver.withInput(new LongWritable(12345L), new Text("3s1rvMFUweQ,17.14.11,\"Taylor Swift: …Ready for It? (Live) - SNL\",\"Saturday Night Live\",24,2017-11-12T06:24:44.000Z,\"SNL\"|\"Saturday Night Live\"|\"SNL Season 43\"|\"Episode 1730\"|\"Tiffany Haddish\"|\"Taylor Swift\"|\"Taylor Swift Ready for It\"|\"s43\"|\"s43e5\"|\"episode 5\"|\"live\"|\"new york\"|\"comedy\"|\"sketch\"|\"funny\"|\"hilarious\"|\"late night\"|\"host\"|\"music\"|\"guest\"|\"laugh\"|\"impersonation\"|\"actor\"|\"improv\"|\"musician\"|\"comedian\"|\"actress\"|\"If Loving You Is Wrong\"|\"Oprah Winfrey\"|\"OWN\"|\"Girls Trip\"|\"The Carmichael Show\"|\"Keanu\"|\"Reputation\"|\"Look What You Made Me Do\"|\"ready for it?\",1053632,25561,2294,2757,https://i.ytimg.com/vi/3s1rvMFUweQ/default.jpg,False,False,False,\"Musical guest Taylor Swift performs …Ready for It? on Saturday Night Live.\\n\\n#SNL #SNL43\\n\\nGet more SNL: http://www.nbc.com/saturday-night-live\\nFull Episodes: http://www.nbc.com/saturday-night-liv...\\n\\nLike SNL: https://www.facebook.com/snl\\nFollow SNL: https://twitter.com/nbcsnl\\nSNL Tumblr: http://nbcsnl.tumblr.com/\\nSNL Instagram: http://instagram.com/nbcsnl \\nSNL Pinterest: http://www.pinterest.com/nbcsnl/\""));
        mapDriver.withOutput(new Text("\"Saturday Night Live\""), new Text("1053632-25561-2294-2757"));
        mapDriver.runTest();
    }

    @Test
    public void map_withInvalidExampleLine_ignoresLineAndProducesNoOutput()
    {
        mapDriver.withInput(new LongWritable(12345L), new Text("3s1rvMFUweQ,17.14.11,\"Taylor Swift: …Ready for It? (Live) - SNL\",\"Saturday Night Live\""));
        mapDriver.runTest();
    }
}