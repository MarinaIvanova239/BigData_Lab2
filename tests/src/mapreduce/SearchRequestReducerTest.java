package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

public class SearchRequestReducerTest {

    private Reducer<Text, DocumentInfo, Text, TextArrayWritable> reducer;
    private ReduceDriver<Text, DocumentInfo, Text, TextArrayWritable> driver;

    private static final DecimalFormat DF = new DecimalFormat("###.########");

    @Before
    public void testSetup() {
        reducer = new SearchRequestReducer();
        driver = new ReduceDriver<Text, DocumentInfo, Text, TextArrayWritable>(reducer);
    }

    @Test
    public void testMultiWords() {
        List<Pair<Text, TextArrayWritable>> out = null;

        List<DocumentInfo> values = new ArrayList<DocumentInfo>();
        values.add(new DocumentInfo(new Text("in1.txt"), 5, 8, 5.0/8.0, 0.0));
        values.add(new DocumentInfo(new Text("in2.txt"), 2, 13, 2.0/13.0, 0.0));
        values.add(new DocumentInfo(new Text("in3.txt"), 9, 10, 9.0/10.0, 0.0));

        ArrayList<Text> fileList = new ArrayList<Text>();
        fileList.add(new Text( "in1.txt" + " , " + DF.format(5.0/8.0 * Math.log(21 / 3.0) )));
        fileList.add(new Text( "in3.txt" + " , " + DF.format(9.0/10.0 * Math.log(21 / 3.0) )));

        try {
            out = driver.withInput(new Text("hello"), values).run();
        } catch (IOException ioe) {
            fail();
        }

        TextArrayWritable result = new TextArrayWritable(fileList.toArray(new Text[fileList.size()]));
        List<Pair<Text, TextArrayWritable>> expected = new ArrayList<Pair<Text, TextArrayWritable>>();
        expected.add(new Pair<Text, TextArrayWritable>(new Text("hello"), result));
        assertListEquals(expected, out);
    }
}
