package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.porterStemmer;

public class SearchRequestMapper extends Mapper<LongWritable, Text, Text, Text>  {

    private Text stemWord = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        SnowballStemmer stemmer = new porterStemmer();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            stemmer.setCurrent(word);
            stemmer.stem();
            stemWord.set(stemmer.getCurrent());
            context.write(stemWord, new Text(fileName));
        }
    }
}
