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
        ArrayList<Text> fileList = new ArrayList<Text>();

        int numberDocumentsWithToken = 0;
        for (DocumentInfo v : values) {
            numberDocumentsWithToken++;
        }

        for (DocumentInfo v : values) {
            v.setIdf(Math.log(numberDocumentsInCorpus / (double) numberDocumentsWithToken));
            double tfidf = v.getTf() * v.getIdf();
            if (tfidf > threshold) {
                Text newElem = new Text("[" + v.getFileName() + " , " + DF.format(tfidf) + "]");
                fileList.add(newElem);
            }
        }
        context.write(key, new TextArrayWritable(fileList.toArray(new Text[fileList.size()])));
    }
}