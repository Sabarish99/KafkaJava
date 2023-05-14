package org.example;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        //System.out.println("Hello world!");

//        logger.info("My first LOG");LOG

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //crearte Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create ProducerRecord
        for(int i=0;i<10;i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("testTopic", "hello world" + i);

            logger.info("Producer Record created");

            //send data asynchronously ... We need to send record to kafka via producerRecord, but it actually doesn't wait until its sent
            //therefore if we stop with just send(), producer record will never reach Kafka, ie program will be shut down even before the producer had chance to send record to kafka

            producer.send(producerRecord);

        }
        //flush data - sync call

        //due to above reason, we use flush(); flush is sync operation which waits on this line until all producer records are sent to kafka
        producer.flush();

        producer.close();
    }
}