package mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchRequestReducer extends Reducer<Text, DocumentInfo, Text, TextArrayWritable> {

    private static final DecimalFormat DF = new DecimalFormat("###.########");
    private final double threshold = 0.5;

    @Override
    public void reduce(Text key, Iterable<DocumentInfo> values, Context context)
            throws IOException, InterruptedException {

        int numberDocumentsInCorpus = Integer.parseInt(context.getJobName());
        //int numberDocumentsInCorpus = 3;
        ArrayList<Text> fileList = new ArrayList<Text>();

        // count number of documents which contain current word
        int numberDocumentsWithToken = 0;
        for (DocumentInfo v : values) {
            numberDocumentsWithToken++;
        }

        // count idf and tf-idf and save document, depending on comparison with threshold
        for (DocumentInfo v : values) {
            v.setIdf(Math.log(numberDocumentsInCorpus / (double) numberDocumentsWithToken));
            double tfidf = v.getTf() * v.getIdf();
            if (tfidf > threshold) {
                Text newElem = new Text( Long.toString(v.getFileIndex()) + " , " + DF.format(tfidf) );
                fileList.add(newElem);
            }
        }

        // save result to context
        context.write(key, new TextArrayWritable(fileList.toArray(new Text[fileList.size()])));
    }
}