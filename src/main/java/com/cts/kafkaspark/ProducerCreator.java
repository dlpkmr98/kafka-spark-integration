/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkaspark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author dlpkmr98
 */
public class ProducerCreator {

    static void readDataFromDir(Producer<String, String> producer) throws FileNotFoundException, IOException {
        String topic = Configuration.topic1;
        String dirPath = "C:\\Users\\dlpkmr98\\Desktop\\kafkadata";
        int count = 0;
        File dir = new File(dirPath);
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    Charset UTF8 = Charset.forName("UTF-8");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.getAbsolutePath()), UTF8));
                    while (true) {
                        String line = reader.readLine();
                        if (line == null) {
                            break;
                        }
                        if (line.equals("")) {
                        } else {
                            count++;

                            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                            try {

                                producer.send(record, new Callback() {
                                    public void onCompletion(RecordMetadata metadata, Exception e) {
                                        if (e != null) {

                                            System.out.println("Send failed for record {}" + record.value() + " " + e);
                                        } else {
                                            System.out.println("send successfully…." + record.value());
                                        }
                                    }
                                });

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        }
                    }
                }
            }
        }

    }
}
