package mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchRequestReducer extends Reducer<Text, DocumentInfo, Text, TextArrayWritable> {

    private final double threshold = 0.5;

    @Override
    public void reduce(Text key, Iterable<DocumentInfo> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int numberDocumentsInCorpus = conf.getInt("numFiles", 1);
        ArrayList<Text> fileList = new ArrayList<Text>();

        // count number of documents which contain current word
        int numberDocumentsWithToken = 0;
        Map<Long, Double> result = new HashMap<>();

        // count number of documents with current token
        for (DocumentInfo v : values) {
            result.put(v.getFileIndex(), v.getTf());
            numberDocumentsWithToken++;
        }

        double idf = Math.log(numberDocumentsInCorpus / (double) numberDocumentsWithToken);

        List<Map.Entry<Long, Double>> list =
                new LinkedList<Map.Entry<Long, Double>>( result.entrySet() );

        for (int i = 0; i < list.size(); i++) {
            double tfidf = list.get(i).getValue() * idf;
            if (tfidf > threshold) {
                Text newElem = new Text( Long.toString(list.get(i).getKey()) + " , " + Double.toString(tfidf) );
                fileList.add(newElem);
            }
        }

        // save result to context
        if (fileList.size() != 0)
            context.write(key, new TextArrayWritable(fileList.toArray(new Text[fileList.size()])));
    }
}