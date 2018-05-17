package com.airline.project.kafka;

import java.io.FileReader;


//Utilities of Java
import java.util.Properties;


//Kafka producer classes
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class KafkaAirlineProducer extends Thread{

    private final KafkaProducer<String,String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final String inputData;

    public KafkaAirlineProducer(String topic, Boolean isAsync, String inputData){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaAirlineProperties.KAFKA_SERVER_URL+":"+KafkaAirlineProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"SparkAirlineProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        /*
        This is the number of messages producer can send to brokers without waiting for a response.
        If you do not care about the order of the messages, then setting its value to more than 1 will increase throughput.
        */
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,KafkaAirlineProperties.MAX_FLIGHT_CONNECTION_VALUE);
        /*
        Producer will only receive acknowledgment when the leader has received acknowledgment for all the replicas successfully.
        */
        props.put(ProducerConfig.ACKS_CONFIG,KafkaAirlineProperties.ACKS_VALUE);
        /*
        If message sending fails, this represents the number of times producer will retry sending messages before it throws an exception.
        */
        props.put(ProducerConfig.RETRIES_CONFIG,KafkaAirlineProperties.RETRY_VALUE);
        /*
        This represents an amount of time that a producer should wait for additional messages before sending a current batch to the broker.
        */
        props.put(ProducerConfig.LINGER_MS_CONFIG,KafkaAirlineProperties.LINGER_VALUE);
        /*
        This setting allows the producer to batch the messages based on the partition up to the configured amount of size.
        When the batch reaches the limit, all messages in the batch will be sent.
        */
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,KafkaAirlineProperties.MSG_BATCH_VALUE);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,KafkaAirlineProperties.KAFKA_PRODUCER_BUFFER_SIZE);

        //Enable the Kafka Producer with the Given Properties
        producer = new KafkaProducer<String, String>(props);

        this.topic = topic;
        this.isAsync = isAsync;
        this.inputData = inputData;

    }

    public void sendMessage(String key, String value){
        long startTime = System.currentTimeMillis();
        if (isAsync) {
            //Send Asynchronously
            producer.send(new ProducerRecord<String, String>(topic,key),
                    (Callback) new DemoCallBack(startTime,key,value));
        }
        else {
            //Send Synchronously
            try {
                producer.send(
                        new ProducerRecord<String, String>(topic,key,value))
                        .get();
                System.out.println("Sent Message: (" + key + ", " + value + ")");
            } catch (InterruptedException e){
                e.printStackTrace();
            } catch (ExecutionException e){
                e.printStackTrace();
            }

        }


    }

    public void run(){

        int messageNo = 0;
        FileInputStream fis;
        BufferedReader br = null;
        while(true) {
            try {
                fis = new FileInputStream(this.inputData);
                br = new BufferedReader(new InputStreamReader(fis));
                String line = null;
                while ((line = br.readLine()) != null){
                    messageNo++;
                    this.sendMessage(messageNo+"",line);
                }

            } catch (Exception e){
                e.printStackTrace();
            } finally {
                try {
                    br.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
    }

    class DemoCallBack implements Callback{
        private long startTime;
        private String key;
        private String message;

        public DemoCallBack(long startTime,String key,String message){
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling
         * of request completion. This method will be called when the record sent to
         * the server has been acknowledged. Exactly one of the argument will be non-null.
         */

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (recordMetadata != null){
                System.out.println("message(" + key + ", " + message
                + ") sent to partition(" + recordMetadata.partition() + "), "
                + "offest(" + recordMetadata.offset() + ") in " + elapsedTime
                + " ms");
            }
            else {
                e.printStackTrace();
            }

        }
    }



}
