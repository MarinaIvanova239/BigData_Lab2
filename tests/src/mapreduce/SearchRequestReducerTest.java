package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import static mapreduce.CustomComparison.listsArrayAreEqual;
import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SearchRequestReducerTest {

    private Reducer<Text, DocumentInfo, Text, TextArrayWritable> reducer;
    private ReduceDriver<Text, DocumentInfo, Text, TextArrayWritable> driver;
    Configuration conf = new Configuration();

    @Before
    public void testSetup() {
        reducer = new SearchRequestReducer();
        driver = new ReduceDriver<Text, DocumentInfo, Text, TextArrayWritable>(reducer);
    }

    @Test
    public void testMultiWords() {
        conf.setInt("numFiles", 21);
        driver.setConfiguration(conf);

        List<Pair<Text, TextArrayWritable>> out = new ArrayList<Pair<Text, TextArrayWritable>>();

        ArrayList<DocumentInfo> values = new ArrayList<DocumentInfo>();
        values.add(new DocumentInfo(1L, 5, 8, 5.0/8.0, 0.0));
        values.add(new DocumentInfo(2L, 2, 13, 2.0/13.0, 0.0));
        values.add(new DocumentInfo(3L, 9, 10, 9.0/10.0, 0.0));

        ArrayList<Text> fileList = new ArrayList<Text>();
        fileList.add(new Text( Long.toString(1L) + " , " + Double.toString(5.0/8.0 * Math.log(21 / 3.0) )));
        fileList.add(new Text( Long.toString(3L) + " , " + Double.toString(9.0/10.0 * Math.log(21 / 3.0) )));

        try {
            out = driver.withInput(new Text("hello"), values).run();
        } catch (IOException ioe) {
            fail();
        }

        TextArrayWritable result = new TextArrayWritable(fileList.toArray(new Text[fileList.size()]));
        List<Pair<Text, TextArrayWritable>> expected = new ArrayList<Pair<Text, TextArrayWritable>>();
        expected.add(new Pair<Text, TextArrayWritable>(new Text("hello"), result));
        assertEquals(true, listsArrayAreEqual(expected, out));
    }
}
