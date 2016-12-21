package mapreduce;

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
import static org.junit.Assert.fail;

public class SearchRequestMapReduceTest {

    private Mapper<LongWritable, Text, Text, DocumentInfo> mapper;
    private Reducer<Text, DocumentInfo, Text, TextArrayWritable> reducer;
    private MapReduceDriver<LongWritable, Text, Text, DocumentInfo, Text, TextArrayWritable> driver;

    private static final DecimalFormat DF = new DecimalFormat("###.########");

    @Before
    public void testSetup() {
        mapper = new SearchRequestMapper();
        reducer = new SearchRequestReducer();
        driver = new MapReduceDriver<LongWritable, Text, Text, DocumentInfo, Text, TextArrayWritable>();
        driver.setMapper(mapper);
        driver.setReducer(reducer);
    }

    @Test
    public void testInvertIndex() {
        List<Pair<Text, TextArrayWritable>> out = null;

        try {
            out = driver.withInput(new LongWritable(0), new Text("hello world hello")).run();
        } catch (IOException ioe) {
            fail();
        }

        List<Pair<Text, TextArrayWritable>> expected = new ArrayList<Pair<Text, TextArrayWritable>>();
        expected.add(new Pair<Text, TextArrayWritable>(new Text("hello"),  new TextArrayWritable(null)));

        assertListEquals(expected, out);
    }
}
