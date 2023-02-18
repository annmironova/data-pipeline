package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class RowInserter {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        RowInserter inserter = new RowInserter();
        String brokers = "localhost:29092";
        String topicName = "uci_news";
        SparkSession spark = inserter.getSparkSession();
        Dataset<Row> datasetTransformed = inserter.readStreamingDatasetWithSchema(brokers, topicName, spark);

        String user = "postgres";
        String password = "postgres";
        String url = "jdbc:postgresql://localhost:5432/postgres";

        StreamingQuery streamingQuery = datasetTransformed.writeStream().foreachBatch(
                (dataset, batchId) -> {
                    Properties connectionProperties = new Properties();
                    connectionProperties.put("user", user);
                    connectionProperties.put("password", password);

                    dataset.write()
                            .mode(SaveMode.Overwrite)
                            .jdbc(url, "test.uc_news_autocreated", connectionProperties);

                }
        ).trigger(Trigger.ProcessingTime("2 seconds")).start();

        streamingQuery.awaitTermination();

    }

    private Dataset<Row> readStreamingDatasetWithSchema(String brokers, String topicName, SparkSession spark) {
        Dataset<Row> dataset = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topicName)
                .load();

        StructType schema = getSchema();

        Dataset<Row> datasetTransformed = dataset
                .selectExpr("CAST(value as string)")
                .select(from_json(col("value"), schema))
                .select("from_json(value).*");
        return datasetTransformed;
    }

    @NotNull
    private StructType getSchema() {
        StructType schema = new StructType(new StructField[]{
                new StructField("ID", DataTypes.StringType, false, Metadata.empty()),
                new StructField("TITLE", DataTypes.StringType, false, Metadata.empty()),
                new StructField("URL", DataTypes.StringType, false, Metadata.empty()),
                new StructField("PUBLISHER", DataTypes.StringType, false, Metadata.empty()),
                new StructField("CATEGORY", DataTypes.StringType, false, Metadata.empty()),
                new StructField("STORY", DataTypes.StringType, false, Metadata.empty()),
                new StructField("HOSTNAME", DataTypes.StringType, false, Metadata.empty()),
                new StructField("TIMESTAMP", DataTypes.StringType, false, Metadata.empty())
        });
        return schema;
    }

    private SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();
        return spark;
    }

}
