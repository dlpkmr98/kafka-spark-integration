/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkaspark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;


import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 *
 * @author dlpkmr98
 */
public class SparkConsumer {

    public static void main(String[] args) throws InterruptedException {

//Set kafka consumer properties
        HashMap<String, String> kafkaParams = Configuration.setKafkaConsumerParameter();
//Set spark properties
        SparkConf sparkConf = Configuration.setSparkParameter();
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1));

//Create stream for accessing data from kafka broker on topic jsondata
        JavaPairInputDStream<String, String> jsonData = StreamCreator.createJsonDataStream(jssc, kafkaParams, new HashSet<String>(Arrays.asList(Configuration.topic1.split(","))));
      //  JavaPairInputDStream<String, String> derbyData = StreamCreator.createJsonDataStream(jssc, kafkaParams, new HashSet<String>(Arrays.asList("derbydata".split(","))));
//Read data from stream and store into disk
        jsonData.map(new Function<Tuple2<String, String>, String>() {  
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        }).foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
               if (!rdd.isEmpty()) {
                    //rdd.saveAsTextFile("C:\\Users\\Downloads\\file2.txt");
                    rdd.collect();
                }
               
            }
        });
        
        
        
     
        
        
//
//        derbyData.map(new Function<Tuple2<String, String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tuple2) {
//                return tuple2._2();
//            }
//        }).foreachRDD(new Function<JavaRDD<String>, Void>() {
//            @Override
//            public Void call(JavaRDD<String> rdd) throws Exception {
//                if (!rdd.isEmpty()) {
//                    rdd.saveAsTextFile("C:\\Users\\Downloads\\file3.txt");
//                }
//                return null;
//            }
//        });
        jssc.start();
        jssc.awaitTermination();

    }
}
