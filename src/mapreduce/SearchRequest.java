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
import java.util.*;

public class SearchRequest  extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        // get number of documents in input set and set it as a job name
        Path inputPath = new Path(args[1]);
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);

        conf.setInt("numFiles", stat.length);

        Job job = new Job(conf, "SearchRequest");

        // set classes
        job.setJarByClass(SearchRequest.class);
        job.setMapperClass(SearchRequestMapper.class);
        job.setCombinerClass(SearchRequestCombiner.class);
        job.setReducerClass(SearchRequestReducer.class);

        // set input and output path and classes
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocumentInfo.class);
        job.setOutputValueClass(TextArrayWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String readRequestFile(String fileName) throws Exception {
        InputStream is = new FileInputStream(fileName);
        BufferedReader buf = new BufferedReader(new InputStreamReader(is));

        // read request file to string
        String line = buf.readLine();
        StringBuilder sb = new StringBuilder();
        while(line != null) {
            sb.append(line);
            line = buf.readLine();
        }
        return sb.toString();
    }

    public static String removeChar(String s, char c) {
        String r = "";
        for (int i = 0; i < s.length(); i ++) {
            if (s.charAt(i) != c) r += s.charAt(i);
        }
        return r;
    }

    public static Map<String, Map<Long, Double>> readIndexFile(String fileName) throws Exception {

        Map<String, Map<Long, Double>> index = new HashMap<String, Map<Long, Double>>();

        FileReader input = new FileReader(fileName);
        BufferedReader bufRead = new BufferedReader(input);
        String myLine;

        // from each line of index file, word and document set are got
        while ( (myLine = bufRead.readLine()) != null)
        {
            String[] array1 = myLine.split(":");
            String[] array2 = array1[1].split(";");
            Map<Long, Double> documentList = new HashMap<Long, Double>();

            // get document set with name of document and tf-idf
            for (String elem: array2) {
                String[] array3 = elem.split(",");
                documentList.put(Long.parseLong(array3[0].trim()), Double.parseDouble(array3[1].trim()));
            }

            // add to list word and corresponding document set
            for (int i = 0; i < array2.length; i++) {
                String word = removeChar(array1[0].trim(), '\u0000');
                index.put(word, documentList);
            }
        }

        return index;
    }

    public static List<Map.Entry<Long, Double>> findIntersection(List<Map<Long, Double>> documentSets) {

        // sort first document set by documents' names
        List<Map.Entry<Long, Double>> listFirst =
                new LinkedList<Map.Entry<Long, Double>>( documentSets.get(0).entrySet() );
        Collections.sort(listFirst, new Comparator<Map.Entry<Long, Double>>() {
            public int compare(Map.Entry<Long, Double> firstSet, Map.Entry<Long, Double> secondSet) {
                return firstSet.getKey().compareTo(secondSet.getKey());
            }
        });

        // for each set in list of document sets
        for (Map<Long, Double> set: documentSets) {

            // sort set by names of documents
            List<Map.Entry<Long, Double>> listSecond =
                    new LinkedList<Map.Entry<Long, Double>>( set.entrySet() );
            Collections.sort(listSecond, new Comparator<Map.Entry<Long, Double>>() {
                public int compare(Map.Entry<Long, Double> firstSet, Map.Entry<Long, Double> secondSet) {
                    return firstSet.getKey().compareTo(secondSet.getKey());
                }
            });

            // find intersection with previous result set and current set
            int sizeFirstList = listFirst.size(), sizeSecondList = listSecond.size();
            int indexFirst = 0, indexSecond = 0;
            List<Map.Entry<Long, Double>> resultSet = new LinkedList<Map.Entry<Long, Double>>();
            while (indexFirst < sizeFirstList && indexSecond < sizeSecondList) {
                Map.Entry<Long, Double> first = listFirst.get(indexFirst);
                Map.Entry<Long, Double> second = listSecond.get(indexSecond);

                if (first.getKey().compareTo(second.getKey()) == 0) {
                    Double newTfIdf = (first.getValue() + second.getValue()) / 2.0;
                    first.setValue(newTfIdf);
                    resultSet.add(first);
                    indexFirst++;
                    indexSecond++;
                } else if (first.getKey().compareTo(second.getKey()) < 0) {
                    indexFirst++;
                } else if (first.getKey().compareTo(second.getKey()) > 0) {
                    indexSecond++;
                }
            }

            // write result intersection to first list
            listFirst.clear();
            for (Map.Entry<Long, Double> result: resultSet) {
                listFirst.add(result);
            }
        }

        return listFirst;
    }

    public static void main(String[] args) throws Exception {

        // run MapReduce to create file with invert index
        int res = ToolRunner.run(new Configuration(), new SearchRequest(), args);
        if (res == 1)
            System.exit(1);

        // read request string
        String request = readRequestFile(args[0]);
        StringTokenizer tokenizer = new StringTokenizer(request);
        int numTokens = tokenizer.countTokens();

        SnowballStemmer stemmer = new porterStemmer();

        Map<String, Map<Long, Double>> wordsMap = readIndexFile(args[2] + "/part-r-00000");
        List<Map<Long, Double>> documentSets = new ArrayList<Map<Long, Double>>();

        // stem each word in request string and corresponding set of documents
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            stemmer.setCurrent(word);
            stemmer.stem();
            if (wordsMap.containsKey(stemmer.getCurrent())) {
                Map<Long, Double> documentList = wordsMap.get(stemmer.getCurrent());
                documentSets.add(documentList);
            } else if (wordsMap.containsKey(word.trim())) {
                Map<Long, Double> documentList = wordsMap.get(word);
                documentSets.add(documentList);
            }
        }

        if (documentSets.size() < numTokens )
            System.exit(2);

        // sort list of document sets by size of sets
        Collections.sort(documentSets, new Comparator<Map<Long, Double>>() {
            public int compare(Map<Long, Double> firstSet, Map<Long, Double> secondSet) {
                return firstSet.size() < secondSet.size() ? -1 : 1;
            }
        });

        List<Map.Entry<Long, Double>> result = findIntersection(documentSets);

        // sort result intersection set by tf-idf in reverse order
        Collections.sort(result, new Comparator<Map.Entry<Long, Double>>() {
            public int compare(Map.Entry<Long, Double> firstSet, Map.Entry<Long, Double> secondSet) {
                return secondSet.getValue().compareTo(firstSet.getValue());
            }
        });

        // write result in file "output.txt"
        try{
            PrintWriter writer = new PrintWriter(args[3], "UTF-8");
            for (Map.Entry<Long, Double> entry : result) {
                writer.println(entry.getKey());
            }
            writer.close();
        } catch (IOException e) {
            System.out.println("Error during writing file!");
            throw e;
        }

        System.exit(0);
    }
}