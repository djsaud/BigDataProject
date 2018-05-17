-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/avro.jar;
-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/json-simple-1.1.jar;
-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/piggybank.jar;
-- REGISTER /home/cloudera/Desktop/Spark-Kafka-UseCase/BigData_Projects-1.0-SNAPSHOT_java7.jar;

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
DEFINE UUID com.airline.project.pigudf.UUIDGenerator();

set aggregate.warning true;

carrierData_raw =
            LOAD '/user/cloudera/testAirline/data/carriers.csv'
            USING CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER')
            AS (code:chararray,description:chararray);

filter_carrierData_raw_header = FILTER carrierData_raw  BY ( code != 'Code' );

add_uuid_time_carrierData_raw =
            FOREACH filter_carrierData_raw_header
               GENERATE
                  UUID() as uuid,
                  code as carrierCode,
                  description as description,
                  ToString(CurrentTime(),'yyyy-MM-dd:hh:mm:ss') as curr_timestamp;

STORE add_uuid_time_carrierData_raw
    INTO '/user/cloudera/testAirline/outdata/carrier'
    USING org.apache.pig.piggybank.storage.avro.AvroStorage();