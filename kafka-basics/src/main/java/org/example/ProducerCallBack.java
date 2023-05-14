package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallBack {

    private static final Logger logger = LoggerFactory.getLogger(ProducerCallBack.class.getSimpleName());

    public static void main(String[] args) {
        //System.out.println("Hello world!");

//        logger.info("My first LOG");LOG

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG , org.apache.kafka.clients.producer.internals.DefaultPartitioner.class.getName());

        //crearte Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create ProducerRecord
        for (int i = 30; i < 55; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("testTopic", "key "+i,"welcome to kafka " + i);

            logger.info("Producer Record created");

            //send data asynchronously ... We need to send record to kafka via producerRecord, but it actually doesn't wait until its sent
            //therefore if we stop with just send(), producer record will never reach Kafka, ie program will be shut down even before the producer had chance to send record to kafka

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null)
                        logger.info("Recieved New Meta data  \n" +
                                "Topic " + metadata.topic() + "\n" +
                                "Offset " + metadata.offset() + "\n" +
                                "Partition " + metadata.partition() +"\n"+
                                "Timestamp " + metadata.timestamp());

                    else logger.error(exception.toString());

                }
            });

        }
        //flush data - sync call

        //due to above reason, we use flush(); flush is sync operation which waits on this line until all producer records are sent to kafka
        producer.flush();

        producer.close();

    }
}


