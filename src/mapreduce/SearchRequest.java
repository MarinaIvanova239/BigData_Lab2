package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.porterStemmer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class SearchRequest  extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "SearchRequest");

        job.setJarByClass(SearchRequest.class);
        job.setMapperClass(SearchRequestMapper.class);
        job.setCombinerClass(SearchRequestCombiner.class);
        job.setReducerClass(SearchRequestReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        Path inputPath = new Path(args[1]);
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);

        job.setJobName(String.valueOf(stat.length));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static String readRequestFile(String fileName) throws Exception {
        InputStream is = new FileInputStream(fileName);
        BufferedReader buf = new BufferedReader(new InputStreamReader(is));
        String line = buf.readLine();
        StringBuilder sb = new StringBuilder();
        while(line != null) {
            sb.append(line).append("\n");
            line = buf.readLine();
        }
        return sb.toString();
    }

    private static Map<String, Map<String, Double>> readIndexFile(String fileName) throws Exception {

        Map<String, Map<String, Double>> index = new HashMap<String, Map<String, Double>>();

        FileReader input = new FileReader(fileName);
        BufferedReader bufRead = new BufferedReader(input);
        String myLine;

        while ( (myLine = bufRead.readLine()) != null)
        {
            String[] array1 = myLine.split(":");
            String[] array2 = array1[1].split(";");
            Map<String, Double> documentList = new HashMap<String, Double>();
            for (String elem: array2) {
                String[] array3 = elem.split(",");
                documentList.put(array3[0], Double.parseDouble(array3[1]));
            }

            for (int i = 0; i < array2.length; i++) {
                index.put(array1[0], documentList);
            }
        }

        return index;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SearchRequest(), args);
        if (res == 1)
            System.exit(1);

        String request = readRequestFile(args[0]);
        StringTokenizer tokenizer = new StringTokenizer(request);

        SnowballStemmer stemmer = new porterStemmer();

        Map<String, Map<String, Double>> wordsMap = readIndexFile(args[2]);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            stemmer.setCurrent(word);
            stemmer.stem();
            Map<String, Double> documentList = wordsMap.get(stemmer.getCurrent());
        }

        System.exit(0);
    }
}