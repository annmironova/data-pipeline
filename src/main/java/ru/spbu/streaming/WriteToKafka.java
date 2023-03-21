package ru.spbu.streaming;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class WriteToKafka {
    public static void main(String[] args) {
        String sourceCsvPath = "src/main/resources/uci-news-aggregator.csv";
        String topicName = "uci_news";
        writeKafka(sourceCsvPath, topicName);
    }

    public static void writeKafka(String sourceCsvPath, String topicName) {
        String brokers = "localhost:29092";

        SparkSession spark = SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load(sourceCsvPath);

        Dataset<Row> df1 = df.select(col("ID"), to_json(struct("*"))).toDF("key", "value");

        df1.write().format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("topic", topicName)
                .save();
    }

}
