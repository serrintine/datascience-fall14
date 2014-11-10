package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Rank {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text bigram = new Text();
    private Text word1 = new Text();
    private Text word2 = new Text();
    private int count;
      
    public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter)
    throws IOException {
      String text = value.toString(); 
      List<String> lines = Arrays.asList(text.split("\n"));

      for (String line: lines) {
        String[] words = line.split("\\s+");
        word1.set(words[0]);
        word2.set(words[1]);
        count = Integer.parseInt(words[2]);
        bigram.set(word1 + " " + word2 + " " + count);

        output.collect(word1, bigram);
        output.collect(word2, bigram);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {
      TreeSet<String> ranks = new TreeSet<String>(new Comparator<String>() {
        public int compare(String one, String two) {          
          Integer count1 = Integer.parseInt(one.replaceAll("[^0-9]", ""));
          Integer count2 = Integer.parseInt(two.replaceAll("[^0-9]", ""));
          return count2.compareTo(count1);
        }
      });

      while (values.hasNext()) {
        ranks.add(values.next().toString());
        if (ranks.size() > 5) {
          ranks.remove(ranks.pollLast());
        }
      }

      for (String bigram: ranks) {
        output.collect(key, new Text(bigram));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Rank.class);
    conf.setJobName("rank");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
