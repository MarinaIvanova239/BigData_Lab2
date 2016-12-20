package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

public class SearchRequestCombinerTest {

    private Reducer<Text, DocumentInfo, Text, DocumentInfo> reducer;
    private ReduceDriver<Text, DocumentInfo, Text, DocumentInfo> driver;

    @Before
    public void testSetup() {
        reducer = new SearchRequestCombiner();
        driver = new ReduceDriver<Text, DocumentInfo, Text, DocumentInfo>(reducer);
    }

    @Test
    public void testMultiWords() {
        List<Pair<Text, DocumentInfo>> out = null;

        List<DocumentInfo> values = new ArrayList<DocumentInfo>();
        values.add(new DocumentInfo(new Text("in1.txt"), 1, 15, 0.0, 0.0));
        values.add(new DocumentInfo(new Text("in1.txt"), 1, 15, 0.0, 0.0));
        values.add(new DocumentInfo(new Text("in1.txt"), 1, 15, 0.0, 0.0));

        try {
            out = driver.withInput(new Text("hello"), values).run();
        } catch (IOException ioe) {
            fail();
        }

        DocumentInfo result = new DocumentInfo(new Text("in1.txt"), 3, 15, 3.0 / 15.0, 0.0);
        List<Pair<Text, DocumentInfo>> expected = new ArrayList<Pair<Text, DocumentInfo>>();
        expected.add(new Pair<Text, DocumentInfo>(new Text("hello"), result));
        assertListEquals(expected, out);
    }
}