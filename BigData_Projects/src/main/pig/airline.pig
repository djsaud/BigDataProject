-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/avro.jar;
-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/json-simple-1.1.jar;
-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/piggybank.jar;
-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/BigData_Projects-1.0-SNAPSHOT_java7.jar;

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
DEFINE UUID com.airline.project.pigudf.UUIDGenerator();

set aggregate.warning true;

airportData_raw =
            LOAD '${INPUT_PATH}'
            USING CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER')
            AS (iata:chararray,airport:chararray,city:chararray,state:chararray,country:chararray,lat:double,longi:double);

filter_airportData_raw_header = FILTER airportData_raw  BY ( iata != 'iata' );

add_uuid_time_airportData_raw =
            FOREACH filter_airportData_raw_header
               GENERATE
                  UUID() as uuid,
                  iata as iata,
                  airport as airport,
                  city as city,
                  state as state,
                  country as country,
                  lat as latitude,
                  longi as longitude,
                  ToString(CurrentTime(),'yyyy-MM-dd:hh:mm:ss') as curr_timestamp;

STORE add_uuid_time_airportData_raw
    INTO '${OUTPUT_PATH}'
    USING AvroStorage;