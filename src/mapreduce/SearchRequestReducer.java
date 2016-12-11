package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchRequestReducer extends Reducer<Text, Text, Text, TextArrayWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<Text> fileList = new ArrayList<Text>();
        for (Text v : values) {
            if (!fileList.contains(v))
                fileList.add(v);
        }
        context.write(key, new TextArrayWritable(fileList.toArray(new Text[fileList.size()])));
    }
}