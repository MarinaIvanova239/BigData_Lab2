package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SearchRequestCombiner extends Reducer<Text, DocumentInfo, Text, DocumentInfo> {

    @Override
    public void reduce(Text key, Iterable<DocumentInfo> values, Context context)
            throws IOException, InterruptedException {

        // count number of same words in one document
        Text fileName = null;
        int numberWords = 0;
        int sum = 0;
        for (DocumentInfo v : values) {
            fileName = v.getFileName();
            numberWords = v.getNumberWords();
            sum += v.getNumberToken();
        }

        // count tf and save information to context
        double tf = sum / ((double) numberWords);
        context.write(key, new DocumentInfo(fileName, sum, numberWords, tf, 0.0));
    }
}
