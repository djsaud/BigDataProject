package com.airline.project.kafka;

public class KafkaAirlineMain {

    public static void main(String [] args){

        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        KafkaAirlineProducer airlineProducerThread4Aiport =
                new KafkaAirlineProducer(KafkaAirlineProperties.TOPIC1,isAsync,KafkaAirlineProperties.AIRPORT_AIRLINE_DATA);
        airlineProducerThread4Aiport.start();

        KafkaAirlineProducer airlineProducerThread4PlaneData =
                new KafkaAirlineProducer(KafkaAirlineProperties.TOPIC2,isAsync,KafkaAirlineProperties.PLANE_AIRLINE_DATA);
        airlineProducerThread4PlaneData.start();

        KafkaAirlineProducer airlineProducerThread4CarrierData =
                new KafkaAirlineProducer(KafkaAirlineProperties.TOPIC2,isAsync,KafkaAirlineProperties.CARRIER_AIRLINE_DATA);
        airlineProducerThread4PlaneData.start();


    }
}
