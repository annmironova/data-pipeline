package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class RowInserter {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        String brokers = "localhost:29092";
        String topicName = "uci_news";
        String user = "postgres";
        String password = "postgres";
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String table = "test.uc_news_autocreated";
        rowInserter(brokers, topicName, user, password, url, table);
    }


    public static void rowInserter(String brokers, String topicName, String user, String password, String url, String table)
            throws StreamingQueryException, TimeoutException {
        RowInserter inserter = new RowInserter();
        SparkSession spark = inserter.getSparkSession();
        StreamingQueryListener streamingQueryListener = inserter.getStreamingQueryListener();
        spark.streams().addListener(streamingQueryListener);
        Dataset<Row> dataset = inserter.readStreamingDataset(brokers, topicName, spark);
        Dataset<Row> datasetTransformed = inserter.transformStreamingDataset(dataset);
        StreamingQuery streamingQuery = inserter.writeStreamingDataset(user, password, url, table, datasetTransformed);
        streamingQuery.awaitTermination();
    }

    public StreamingQuery writeStreamingDataset(String user, String password, String url, String table, Dataset<Row> datasetTransformed)
            throws TimeoutException{
        StreamingQuery streamingQuery = datasetTransformed.writeStream().foreachBatch(
                (ds, batchId) -> {
                    Properties connectionProperties = new Properties();
                    connectionProperties.put("user", user);
                    connectionProperties.put("password", password);

                    ds.write()
                            .mode(SaveMode.Overwrite)
                            .jdbc(url, table, connectionProperties);
                }
        ).trigger(Trigger.ProcessingTime("2 seconds")).start();
        return streamingQuery;
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

    public SparkSession getSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();
        return spark;
    }

    public StreamingQueryListener getStreamingQueryListener() {
        StreamingQueryListener streamingQueryListener = new StreamingQueryListener() {
            @Override
            public void onQueryStarted(StreamingQueryListener.QueryStartedEvent queryStarted) {
            }
            @Override
            public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent queryTerminated) {
            }
            @Override
            public void onQueryProgress(StreamingQueryListener.QueryProgressEvent queryProgress) {
            }
        };
        return streamingQueryListener;
    }
}
