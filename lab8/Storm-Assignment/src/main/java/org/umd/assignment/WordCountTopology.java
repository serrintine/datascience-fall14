/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.umd.assignment;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.umd.assignment.spout.RandomSentenceSpout;
import org.umd.assignment.spout.TwitterSampleSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
  public static class SplitSentence extends ShellBolt implements IRichBolt {

    public SplitSentence() {
      super("python", "splitsentence.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      
        // ----------------- Task 2    ---------------------------------
        //
        //
        //  Modify this code to exclude stop-words from counting.
        //  Stopword list is provided in the lab folder. 
        //
        //
        // ---------------------------------------------------------
        
        Set<String> stopWords = new HashSet<String>();
        
        try {
            String in = "/home/serrintine/Repos/datascience-fall14/lab8/Stopwords.txt";
            BufferedReader br = new BufferedReader(new FileReader(in));
        
            String sw = br.readLine();
            while (sw != null) {
                stopWords.add(sw.trim());
                sw = br.readLine();
            }
                
            br.close();
        } catch (IOException e) {
            System.exit(-1);
        }

        String word = tuple.getString(0);
        if (!stopWords.contains(word) && !word.toLowerCase().contains("obama")) {
            Integer count = counts.get(word);
            
            if (count == null) {
                count = 0;
            }
            
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }
    }

    @Override
    public void cleanup()
    {
        // ------------------------  Task 3 ---------------------------------------
        //
        //
        //  This function gets called when the Stream processing finishes.
        //  MODIFY this function to print the most frequent words that co-occur 
        //  with Obama [The TwitterSimpleSpout already gives you Tweets that contain
        //  the word obama].
        //
        //  Since multiple threads will be doing the same cleanup operation, writing the
        //  output to a file might not work as desired. One way to do this would be
        //  print the output (using System.out.println) and do a grep/awk/sed on that.
        //
        //--------------------------------------------------------------------------
        
        List<Entry<String, Integer>> top = findTop(counts);
      
        for (Entry<String, Integer> word : top) {
            System.out.println(word.getKey() + " " + word.getValue());
        }
    }
    
    private static List<Entry<String, Integer>> findTop(Map<String, Integer> map) {        
        List<Entry<String, Integer>> result = new ArrayList<Entry<String, Integer>>();
        Set<Entry<String, Integer>> entries = map.entrySet();
        PriorityQueue<Entry<String, Integer>> top = new PriorityQueue<Entry<String, Integer>>(10,
            new Comparator<Entry<String, Integer>>() {
                public int compare(Entry<String, Integer> one, Entry<String, Integer> two) {
                    return (two.getValue()).compareTo(one.getValue());
                }
            }
        );
        
        for (Entry<String, Integer> entry : entries) {
            top.offer(entry);
            if (top.size() > 10) {
                top.poll();
            }
        }

        while (top.size() > 0) {
            result.add(top.poll());
        }
        
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    // ---------------------------- Task 1 -------------------------------------
    //
    //        You need to use TwitterSampleSpout() for the assignemt. But, it won't work
    //        unless you set up the access token correctly in the TwitterSampleSpout.java
    //
    //        RandomSentenceSpout() simply spits out a random sentence. 
    //
    //--------------------------------------------------------------------------

    // Setting up a spout
    //builder.setSpout("spout", new RandomSentenceSpout(), 3);
    builder.setSpout("spout", new TwitterSampleSpout(), 3);

    // Setting up bolts
    builder.setBolt("split", new SplitSentence(), 3).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 3).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      // --------------------------- Task 4 ---------------------------------
      //
      //    The sleep time simply indicates for how long you want to keep your
      //    system up and running. 10000 (miliseconds) here means 10 seconds.
      //     
      //
      // ----------------------------------------------------------------------

      //Thread.sleep(10000);
      Thread.sleep(600000);

      cluster.shutdown(); // blot "cleanup" function is called when cluster is shutdown (only works in local mode)
    }
  }
}
