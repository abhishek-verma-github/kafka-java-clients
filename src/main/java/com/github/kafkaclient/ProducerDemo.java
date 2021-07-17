package com.github.kafkaclient;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.PrimitiveIterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class ProducerDemo{
    public static void main(String[] args){
        //properties
        String bootstrapserver = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer <Key, Value>
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // create producer record
        try {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", "message ID: " + (i + 1));
                TimeUnit.SECONDS.sleep(1);
                producer.send(record);
//                producer.flush();
            }
        }
        catch(InterruptedException ie){
            System.out.println("Exception: "+ ie);
        }
        catch(Exception e){
            System.out.println("Exception:" + e);
        }
        finally {
            System.out.println("Closing Producer...");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.close();
        }
    }
}