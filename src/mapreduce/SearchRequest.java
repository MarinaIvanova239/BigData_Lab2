package mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SearchRequest {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(SearchRequest.class);
        job.setJobName("SearchRequest");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SearchRequestMapper.class);
        job.setCombinerClass(SearchRequestReducer.class);
        job.setReducerClass(SearchRequestReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}