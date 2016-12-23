package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import javax.print.Doc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static mapreduce.CustomComparison.listsDocsAreEqual;
import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        values.add(new DocumentInfo(1L, 1, 15, 0.0, 0.0));
        values.add(new DocumentInfo(1L, 1, 15, 0.0, 0.0));
        values.add(new DocumentInfo(1L, 1, 15, 0.0, 0.0));

        try {
            out = driver.withInput(new Text("hello"), values).run();
        } catch (IOException ioe) {
            fail();
        }

        DocumentInfo result = new DocumentInfo(1L, 3, 15, 3.0 / 15.0, 0.0);
        List<Pair<Text, DocumentInfo>> expected = new ArrayList<Pair<Text, DocumentInfo>>();
        expected.add(new Pair<Text, DocumentInfo>(new Text("hello"), result));
        assertEquals(true, listsDocsAreEqual(expected, out));
    }
}
