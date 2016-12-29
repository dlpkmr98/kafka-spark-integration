/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkaspark;

import java.util.HashMap;
import java.util.Properties;
import org.apache.spark.SparkConf;

/**
 *
 * @author dlpkmr98
 */
public class Configuration {

    public static final String topic1 = "filedata";
    public static final String topic2 = "derbydata";

    static Properties setKafkaProducerParameter() {
        Properties kafkaParams = new Properties();
        kafkaParams.put("zookeeper.connect", "localhost:2181");
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "test");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return kafkaParams;
    }

    static HashMap<String, String> setKafkaConsumerParameter() {
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zk.connect", "localhost:2181");
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("partition", "1");
        return kafkaParams;
    }

    static SparkConf setSparkParameter() {
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.setAppName("Kafka-Spark-Streaming Application");
        sparkConf.setMaster("local");
        sparkConf.set("spark.driver.memory", "1g");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //System.setProperty("hadoop.home.dir", "C:\\Users\\dkuma276\\Downloads\\");
        return sparkConf;
    }

}
