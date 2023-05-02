package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class StreamingPipeline {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        String brokers = "localhost:29092";
        String topicName = "uci_news1";
        String user = "postgres";
        String password = "postgres";
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String table = "test.uc_news_autocreated1";
        rowInserter(brokers, topicName, user, password, url, table);
    }


    public static void rowInserter(String brokers, String topicName, String user, String password, String url, String table)
            throws StreamingQueryException, TimeoutException {
        StreamingPipeline inserter = new StreamingPipeline();
        SparkSession spark = inserter.getSparkSession();
        Dataset<Row> dataset = inserter.readStreamingDataset(brokers, topicName, spark);
        Dataset<Row> datasetTransformed = inserter.transformStreamingDataset(dataset);
        StreamingQuery streamingQuery = inserter.writeStreamingDataset(user, password, url, table, datasetTransformed, brokers);
        streamingQuery.awaitTermination();
    }

    public StreamingQuery writeStreamingDataset(String user,
                                                String password,
                                                String url,
                                                String table,
                                                Dataset<Row> datasetTransformed,
                                                String brokers)
            throws TimeoutException {
        return datasetTransformed.writeStream().foreachBatch(
                (ds, batchId) -> {

                    ds.persist();

                    Dataset<Row> validRecords = ds.filter("ID is NULL and ID_STR is null or ID is not null")
                            .drop("ID_STR")
                            .drop("original_value");


                    Dataset<Row> invalidRecords = ds.filter("ID is NULL and ID_STR is not null")
                            .select(col("original_value").as("value"));

                    Properties connectionProperties = new Properties();
                    connectionProperties.put("user", user);
                    connectionProperties.put("password", password);

                    Dataset<Row> orderedValidRecords = validRecords
                            .select("ID", "TITLE", "URL", "PUBLISHER", "CATEGORY", "STORY", "HOSTNAME", "TIMESTAMP");
                    orderedValidRecords.write()
                            .mode(SaveMode.Append)
                            .jdbc(url, table, connectionProperties);

                    invalidRecords.write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", brokers)
                            .option("topic", "dlq")
                            .save();

                    ds.unpersist();
                }
        ).trigger(Trigger.ProcessingTime("2 seconds")).start();
    }

    public Dataset<Row> readStreamingDataset(String brokers, String topicName, SparkSession spark) {
        Dataset<Row> dataset = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topicName)
                .load();

        return dataset;
    }

    public Dataset<Row> transformStreamingDataset(Dataset<Row> dataset) {
        StructType schema = getSchema();
        return dataset
                .selectExpr("CAST(value as string)")
                .select(from_json(col("value"), schema), col("value").as("original_value"))
                .select("from_json(value).*", "original_value")
                .withColumnRenamed("ID", "ID_STR")
                .withColumn("ID", col("ID_STR").cast(DataTypes.LongType));
    }

    @NotNull
    private StructType getSchema() {
        return new StructType(new StructField[]{
                new StructField("ID", DataTypes.StringType, false, Metadata.empty()),
                new StructField("TITLE", DataTypes.StringType, false, Metadata.empty()),
                new StructField("URL", DataTypes.StringType, false, Metadata.empty()),
                new StructField("PUBLISHER", DataTypes.StringType, false, Metadata.empty()),
                new StructField("CATEGORY", DataTypes.StringType, false, Metadata.empty()),
                new StructField("STORY", DataTypes.StringType, false, Metadata.empty()),
                new StructField("HOSTNAME", DataTypes.StringType, false, Metadata.empty()),
                new StructField("TIMESTAMP", DataTypes.StringType, false, Metadata.empty())
        });
    }

    public SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();
    }
}
