package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SearchRequestMapReduceTest {

    private Mapper<LongWritable, Text, Text, DocumentInfo> mapper;
    private Reducer<Text, DocumentInfo, Text, TextArrayWritable> reducer;
    private Reducer<Text, DocumentInfo, Text, DocumentInfo> combiner;
    private MapReduceDriver<LongWritable, Text, Text, DocumentInfo, Text, TextArrayWritable> driver;
    Configuration conf = new Configuration();

    @Before
    public void testSetup() {
        mapper = new SearchRequestMapper();
        reducer = new SearchRequestReducer();
        combiner = new SearchRequestCombiner();
        driver = new MapReduceDriver<LongWritable, Text, Text, DocumentInfo, Text, TextArrayWritable>();
        driver.setMapper(mapper);
        driver.setReducer(reducer);
        driver.setCombiner(combiner);
    }

    @Test
    public void testMapReduceWork() {

        conf.setInt("numFiles", 10);
        driver.setConfiguration(conf);

        List<Pair<Text, TextArrayWritable>> out = null;

        try {
            out = driver.withInput(new LongWritable(0), new Text("hello world hello")).run();
        } catch (IOException ioe) {
            fail();
        }

        ArrayList<Text> fileListHello = new ArrayList<Text>();
        fileListHello.add(new Text( Long.toString(0L) + " , " + Double.toString(2.0/3.0 * Math.log(10 / 1.0) )));
        TextArrayWritable resultHello = new TextArrayWritable(fileListHello.toArray(new Text[fileListHello.size()]));

        ArrayList<Text> fileListWorld = new ArrayList<Text>();
        fileListWorld.add(new Text( Long.toString(0L) + " , " + Double.toString(1.0/3.0 * Math.log(10 / 1.0) )));
        TextArrayWritable resultWorld = new TextArrayWritable(fileListWorld.toArray(new Text[fileListWorld.size()]));

        List<Pair<Text, TextArrayWritable>> expected = new ArrayList<Pair<Text, TextArrayWritable>>();
        expected.add(new Pair<Text, TextArrayWritable>(new Text("hello"), resultHello));
        expected.add(new Pair<Text, TextArrayWritable>(new Text("world"), resultWorld));

        assertEquals(expected, out);
    }
}
