package com.techno.project.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


public class GenerateSampleJsonData {

    private static final AtomicInteger count = new AtomicInteger(0);
    private String json = "";
    private static final String topicName = "topic1";

    public static Properties loadProperties() {

        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // producer properties
        properties.setProperty("max.in.flight.requests.per.connection", Integer.toString(2));//This is the number of messages producer can send to brokers without waiting for a response. If you do not care about the order of the messages, then setting its value to more than 1 will increase throughput.
        properties.setProperty("timeout.ms", Integer.toString(10000));//This is the amount of time a leader will wait for its followers to acknowledge the message before sending an error to producer. This setting will only help when acks is set to all.
        properties.setProperty("block.on.buffer.full", "true"); //To avoid data loss for the producer
        properties.setProperty("acks", "all");//Producer will only receive acknowledgment when the leader has received acknowledgment for all the replicas successfully.
        properties.setProperty("retries", "2");//If message sending fails, this represents the number of times producer will retry sending messages before it throws an exception.
        properties.setProperty("linger.ms", "1");//This represents an amount of time that a producer should wait for additional messages before sending a current batch to the broker.
        properties.put("batch.size", 20000);//This setting allows the producer to batch the messages based on the partition up to the configured amount of size. When the batch reaches the limit, all messages in the batch will be sent.
        properties.put("buffer.memory", 33554432);
        return properties;

    }

    public static int generateRandomInteger(int aStart, int aEnd, Random aRandom) {
        if (aStart > aEnd) {
            throw new IllegalArgumentException("Start cannot exceed End.");
        }
        // get the range, casting to long to avoid overflow problems
        long range = (long) aEnd - (long) aStart + 1;
        // compute a fraction of the range, 0 <= frac < range
        long fraction = (long) (range * aRandom.nextDouble());

        int randomNumber = (int) (fraction + aStart);

        return randomNumber;
    }

    public void randomDataGen(int noOfRows) {

        Gson gson = new Gson();
        WtransactionData obj = new WtransactionData();
        Producer<String, String> producer = new KafkaProducer<String, String>(loadProperties());

        int END = 2000000;
        int START = 1;
        Random random = new Random();

        ProducerRecord data;

        for (int i = 0; i < noOfRows; i++) {
            int randomNum = generateRandomInteger(START, END, random);
            obj.setId(i);  //count.incrementAndGet()
            obj.setTime_created("1037818147");
            obj.setTime_processed("0");
            obj.setFlags("33554432");
            obj.setAccount_number(randomNum);
            obj.setTransaction_id("TranID" + i);
            obj.setCounterparty("CP" + i);
            obj.setPayee_email("test@testmail.com" + i);
            obj.setAmount("567" + i);
            obj.setStatus("P" + i);
            obj.setType("H" + i);
            obj.setReason("Any Reason " + i);
            obj.setTime_received_payee("TRPayee" + i);
            obj.setTime_created_payer("TCreatePayye" + i);
            obj.setMemo("Memo" + i);
            obj.setPayment_id("27641" + i);
            obj.setAch_id("AchId" + i);
            obj.setSync_group("5621431" + i);
            obj.setAddress_id("addRId" + i);
            obj.setPayee_email_upper("TEST@TESTMAIL.COM" + i);
            obj.setParent_id("1501273");
            obj.setShared_id("9237284218550091776");
            obj.setCctrans_id("CCtrans" + i);
            obj.setCounterparty_alias("CountrPAlaias" + i);
            obj.setCounterparty_alias_type("CounterpartyAliasType");
            obj.setCounterparty_alias_upper("CounterpartyAliasUpperCap");
            obj.setMessage("Test" + i);
            obj.setTime_user("0");
            obj.setMessage_id("MsgId" + i);
            obj.setSubtype("subtype " + i);
            obj.setFlags2("Flags2_" + i);
            obj.setTime_inactive("TInact");
            obj.setTarget_alias_id("Talias");
            obj.setCounterparty_last_login_ip("192.168.0.1");
            obj.setBalance_at_time_created("5783");
            obj.setAccept_deny_method("Test");
            obj.setCurrency_code("Dollar");
            obj.setUsd_amount("1" + i);
            obj.setBase_id("bId" + i);
            obj.setFlags3("Flag3" + i);
            obj.setTime_updated("testtime");
            obj.setTransition("TestTransc");
            obj.setFlags4("Flag_4");
            obj.setTime_row_updated("Testtime");
            obj.setFlags5("Flag5");
            obj.setFlags6("Flag6");
            obj.setFlags7("Flag7");
            json = gson.toJson(obj);

            //Test the Json String generated
            //		System.out.println(json);
            data = new ProducerRecord<String, String>(topicName, "key", json);
            Future<RecordMetadata> recordMetadata = producer.send(data);

            // producer.send(new ProducerRecord(topicName, "Key", json));
        }
        System.out.println("Message sent successfully");
        producer.close();


    }

    public static void main(String[] args) {
        GenerateSampleJsonData objResult = new GenerateSampleJsonData();
        // For test purpose

       // objResult.randomDataGen(Integer.parseInt(args[0]));
        objResult.randomDataGen(100);

    }
}
