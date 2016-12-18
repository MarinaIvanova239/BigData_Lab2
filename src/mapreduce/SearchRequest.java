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

public class SearchRequest  extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "SearchRequest");

        job.setJarByClass(SearchRequest.class);
        job.setMapperClass(SearchRequestMapper.class);
        job.setCombinerClass(SearchRequestCombiner.class);
        job.setReducerClass(SearchRequestReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        Path inputPath = new Path(args[0]);
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);

        job.setJobName(String.valueOf(stat.length));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SearchRequest(), args);



        System.exit(res);
    }
}