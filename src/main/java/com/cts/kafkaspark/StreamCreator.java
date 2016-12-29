/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkaspark;

import java.util.HashMap;
import java.util.HashSet;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 *
 * @author dlpkmr98
 */
public class StreamCreator {

    // Create direct kafka stream with brokers and topics
/* Receive Kafka streaming inputs */
    static JavaPairInputDStream<String, String> createJsonDataStream(JavaStreamingContext jssc, HashMap<String, String> kafkaParams, HashSet<String> topicsSet) {
        JavaPairInputDStream<String, String> jsonData = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
        return jsonData;
    }

}
