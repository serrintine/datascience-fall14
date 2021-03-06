/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.regex.Pattern;

public final class Assignment {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        // Create the context with a 10 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("Assignment");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                new Duration(10000));

        // Create a JavaReceiverInputDStream on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        JavaReceiverInputDStream<String> lines = ssc
            .socketTextStream(
                "localhost", Integer.parseInt("9999"),
                StorageLevels.MEMORY_AND_DISK_SER
            );

        JavaDStream<String> words = lines
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String x) {
                    return Lists.newArrayList(SPACE.split(x));
                }
            });

        JavaPairDStream<String, Integer> wordCounts = words
            .filter(new Function<String, Boolean>() {
                public Boolean call(String s) {
                    return s.equalsIgnoreCase("#obama");
                }
            }).mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) {
                    return new Tuple2<String, Integer>(s.toLowerCase(), 1);
                }
            });

        JavaPairDStream<String, Integer> slidingWindow = wordCounts
            .reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                },
                new Duration(30000),
                new Duration(10000)
            );

        slidingWindow.print();

        ssc.start();

        ssc.awaitTermination();
    }
}
