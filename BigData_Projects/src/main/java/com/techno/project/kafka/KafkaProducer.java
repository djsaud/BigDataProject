package com.techno.project.kafka;

import java.io.FileReader;


//Utilities of Java
import java.util.Properties;
import java.util.concurrent.Future;


//Kafka producer classes
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;


//Google JSON reader Class
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

public class KafkaProducer {

    public static void main(String[] args) throws Exception {

        try {
            String topicName = "topic1";
            JSONParser parser = new JSONParser();
            JSONArray jsonArray = (JSONArray) parser.parse(new FileReader("/home/cloudera/kafka_data/json/student.json"));


            Properties properties = new Properties();
            // kafka bootstrap server
            properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            // producer properties
            properties.setProperty("max.in.flight.requests.per.connection",Integer.toString(2));//This is the number of messages producer can send to brokers without waiting for a response. If you do not care about the order of the messages, then setting its value to more than 1 will increase throughput.
            properties.setProperty("timeout.ms",Integer.toString(10000));//This is the amount of time a leader will wait for its followers to acknowledge the message before sending an error to producer. This setting will only help when acks is set to all.
            properties.setProperty("block.on.buffer.full","true"); //To avoid data loss for the producer
            properties.setProperty("acks", "all");//Producer will only receive acknowledgment when the leader has received acknowledgment for all the replicas successfully.
            properties.setProperty("retries", "2");//If message sending fails, this represents the number of times producer will retry sending messages before it throws an exception.
            properties.setProperty("linger.ms", "1");//This represents an amount of time that a producer should wait for additional messages before sending a current batch to the broker.
            properties.put("batch.size", 20000);//This setting allows the producer to batch the messages based on the partition up to the configured amount of size. When the batch reaches the limit, all messages in the batch will be sent.
            properties.put("buffer.memory",24568545);

            Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

            for (int i = 0; i < jsonArray.size(); i++) {
                Object object = jsonArray.get(i);
                //System.out.println("O/p :-- " + object.toString());

                ProducerRecord data = new ProducerRecord<String, String>(topicName, "key"+i,object.toString());
                Future<RecordMetadata> recordMetadata = producer.send(data);
            }
            producer.close();



        }
        catch (Exception  e) {
            System.out.println(e.getClass());
        }

    }

}
