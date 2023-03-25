package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTransformation {

    //actually failed because of error
    @Test
    public void testTranform() throws TimeoutException {
        RowInserter rowInserter = new RowInserter();
        SparkSession spark = rowInserter.getSparkSession();
        scala.Option<Object> numPartitions = scala.Option.apply(null);
        MemoryStream<String> testStream = new MemoryStream<>(1, spark.sqlContext(), numPartitions, Encoders.STRING());
        String jsonPath = "src/test/resources/test-data.json";
        Dataset<String> personEventDataset = spark.read().json(jsonPath).as(Encoders.STRING());
        testStream.addData(JavaConverters.asScalaIteratorConverter(personEventDataset.collectAsList().iterator()).asScala().toSeq());
        Dataset<String> sessions = testStream.toDS();
        Dataset filteredDF = rowInserter.transformStreamingDataset(sessions.toDF());

        StreamingQuery streamingQuery = filteredDF
                .writeStream()
                .format("memory")
                .queryName("testQuery")
                .outputMode("append")
                .start();
        streamingQuery.processAllAvailable();

        List<Row> result = spark.sql("select * from testQuery").collectAsList();

        assertEquals(1, result.size());
    }
}
