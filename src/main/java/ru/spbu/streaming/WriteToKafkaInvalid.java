package ru.spbu.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import static org.apache.spark.sql.functions.*;

public class WriteToKafkaInvalid {
    public static void main(String[] args) {
        String sourceCsvPath = "src/main/resources/uci-news-aggregator.csv";
        String topicName = "uci_news1";
        writeKafka(sourceCsvPath, topicName);
    }

    public static void writeKafka(String sourceCsvPath, String topicName) {
        String brokers = "localhost:9092";
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer
                <String, String>(props)) {
                producer.send(new ProducerRecord<String, String>(topicName,
                        "test", "{\"ID\":\"test\"}"));
                producer.send(new ProducerRecord<String, String>(topicName,
                        "1", "{\"ID\":\"1\"}"));
        }
    }

}
