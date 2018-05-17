package com.airline.project.kafka;

public class KafkaAirlineProperties {

    public static final String TOPIC1 = "Airports";
    public static final String TOPIC2 = "Planedata";
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final String CLIENT_ID = "AirlineClient";
    public static final String ACKS_VALUE = "all";
    public static final int RETRY_VALUE = 2;
    public static final int LINGER_VALUE = 1;
    public static final int MSG_BATCH_VALUE = 2000;
    public static final int MAX_FLIGHT_CONNECTION_VALUE = 2;
    public static final String AIRPORT_AIRLINE_DATA = "/home/cloudera/Desktop/Spark-Kafka-UseCase/Airline_Data/airports.csv";
    public static final String PLANE_AIRLINE_DATA = "/home/cloudera/Desktop/Spark-Kafka-UseCase/Airline_Data/plane-data.csv";
    public static final String CARRIER_AIRLINE_DATA = "/home/cloudera/Desktop/Spark-Kafka-UseCase/Airline_Data/carriers.csv";

    private KafkaAirlineProperties(){

    }
}
