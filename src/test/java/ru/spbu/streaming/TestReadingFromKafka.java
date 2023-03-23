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
import org.junit.jupiter.api.Test;
import ru.spbu.streaming.WriteToKafka;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestReadingFromKafka {

    private long rows = 0;

    public Thread runRowInserter() {
        String brokers = "localhost:29092";
        String topicName = "test_2";
        String user = "postgres";
        String password = "postgres";
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String table = "test.test_2";
        Thread t = new Thread(() -> {
            try {
                RowInserter inserter = new RowInserter();
                SparkSession spark = inserter.getSparkSession();
                StreamingQueryListener streamingQueryListener = new StreamingQueryListener() {
                    @Override
                    public void onQueryStarted(StreamingQueryListener.QueryStartedEvent queryStarted) {

                    }
                    @Override
                    public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent queryTerminated) {

                    }
                    @Override
                    public void onQueryProgress(StreamingQueryListener.QueryProgressEvent queryProgress) {
                        rows = rows + queryProgress.progress().numInputRows();

                    }
                };

                spark.streams().addListener(streamingQueryListener);
                Dataset<Row> dataset = inserter.readStreamingDataset(brokers, topicName, spark);
                Dataset<Row> datasetTransformed = inserter.transformStreamingDataset(dataset);
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
                streamingQuery.awaitTermination();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return t;
    }

    public void runPipeline() {
        String topicName = "test_2";
        Thread t = runRowInserter();
        t.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        WriteToKafka.writeKafka("src/test/resources/test-data-1.csv", topicName);
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testAvailableTopic() throws TimeoutException, InterruptedException {
        runPipeline();
        assertEquals(5, rows);

    }
}
