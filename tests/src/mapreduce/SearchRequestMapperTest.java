package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SearchRequestMapperTest {

    private Mapper<LongWritable, Text, Text, DocumentInfo> mapper;
    private MapDriver<LongWritable, Text, Text, DocumentInfo> driver;

    @Before
    public void testSetup() {
        Configuration conf = new Configuration();
        conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");
        mapper = new SearchRequestMapper();
        driver = new MapDriver<LongWritable, Text, Text, DocumentInfo>(mapper);
        driver.setConfiguration(conf);
    }

    @Test
    public void testListOfWords() {
        List<Pair<Text, DocumentInfo>> out = null;

        try {
            out = driver.withInput(new LongWritable(0), new Text("hello world hello")).run();
        } catch (IOException ioe) {
            fail();
        }

        DocumentInfo first = new DocumentInfo(0L, 1, 3, 0.0, 0.0);
        DocumentInfo second = new DocumentInfo(0L, 1, 3, 0.0, 0.0);

        List<Pair<Text, DocumentInfo>> expected = new ArrayList<Pair<Text, DocumentInfo>>();
        expected.add(new Pair<Text, DocumentInfo>(new Text("hello"), first));
        expected.add(new Pair<Text, DocumentInfo>(new Text("world"), second));
        expected.add(new Pair<Text, DocumentInfo>(new Text("hello"), first));
        assertEquals(expected, out);
    }

}
