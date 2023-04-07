package ru.spbu.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)

public class TestReadingFromKafka {

    public List<Row> runReading(String topicName, String sourceCsvPath, String queryName, int rows) {
        String brokers = "localhost:29092";
        RowInserter rowInserter = new RowInserter();
        SparkSession spark = rowInserter.getSparkSession();
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    Dataset<Row> dataset = rowInserter.readStreamingDataset(brokers, topicName, spark);
                    Dataset<Row> datasetTransformed = rowInserter.transformStreamingDataset(dataset);
                    StreamingQuery streamingQuery = datasetTransformed
                            .writeStream()
                            .format("memory")
                            .queryName(queryName)
                            .outputMode("append")
                            .start();
                    streamingQuery.processAllAvailable();
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        WriteToKafka.writeKafka(sourceCsvPath, topicName);

        int currentRows = 0;
        long start = System.currentTimeMillis();
        long end = start + 30 * 1000;

        while(currentRows < rows && System.currentTimeMillis() < end) {
            currentRows = spark.sql("select * from " + queryName).collectAsList().size();
        }
        List<Row> result = spark.sql("select * from " + queryName).collectAsList();
        t.interrupt();
        return result;
    }


    @Test
    @DisplayName("Data from existing topic is read")
    public void testReadingFromExistingTopic() {
        String topicName = "r_test_topic_1";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-1.csv";
        String queryName = "rTestQuery1";
        int rows = 5;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);
        boolean isRead = result.size() >= rows;
        assertTrue(isRead);
    }

    @Test
    @DisplayName("Dataset with correct data is read")
    public void testReadingCorrectDataset() {
        String topicName = "r_test_topic_4";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-4.csv";
        String queryName = "rTestQuery4";
        int rows = 5;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com",1234567891));
        expectedResult.add(RowFactory.create(2,"Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com",1234567892));
        expectedResult.add(RowFactory.create(3,"Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com",1234567893));
        expectedResult.add(RowFactory.create(4,"Title 4","http://www.url4.com",
                "Publisher 4","b","abcdef4","www.publisher4.com",1234567894));
        expectedResult.add(RowFactory.create(5,"Title 5","http://www.url5.com",
                "Publisher 5","m","abcdef5","www.publisher5.com",1234567895));

        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null ID is read")
    public void testReadingNullID() {
        String topicName = "r_test_topic_8";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-8.csv";
        String queryName = "rTestQuery8";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(null,"Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com",1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null TITLE is read")
    public void testReadingNullTITLE() {
        String topicName = "r_test_topic_9";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-9.csv";
        String queryName = "rTestQuery9";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,null,"http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com",1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null URL is read")
    public void testReadingNullURL() {
        String topicName = "r_test_topic_10";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-10.csv";
        String queryName = "rTestQuery10";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1",null,
                "Publisher 1","e","abcdef1","www.publisher1.com",1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null PUBLISHER is read")
    public void testReadingNullPUBLISHER() {
        String topicName = "r_test_topic_11";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-11.csv";
        String queryName = "rTestQuery11";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                null,"e","abcdef1","www.publisher1.com",1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with b in CATEGORY is read")
    public void testReadingCategoryB() {
        String topicName = "r_test_topic_14";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-14.csv";
        String queryName = "rTestQuery14";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","b","abcdef1","www.publisher1.com",1234567891));

        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with t in CATEGORY is read")
    public void testReadingCategoryT() {
        String topicName = "r_test_topic_15";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-15.csv";
        String queryName = "rTestQuery15";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","t","abcdef1","www.publisher1.com",1234567891));

        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with english e in CATEGORY is read")
    public void testReadingEngCategoryE() {
        String topicName = "r_test_topic_16";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-16.csv";
        String queryName = "rTestQuery16";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com",1234567891));

        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with m in CATEGORY is read")
    public void testReadingCategoryM() {
        String topicName = "r_test_topic_17";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-17.csv";
        String queryName = "rTestQuery17";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","m","abcdef1","www.publisher1.com",1234567891));

        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null CATEGORY is read")
    public void testReadingNullCATEGORY() {
        String topicName = "r_test_topic_18";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-18.csv";
        String queryName = "rTestQuery18";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1",null,"abcdef1","www.publisher1.com",1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null STORY is read")
    public void testReadingNullSTORY() {
        String topicName = "r_test_topic_22";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-22.csv";
        String queryName = "rTestQuery22";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","e",null,"www.publisher1.com",1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null HOSTNAME is read")
    public void testReadingNullHOSTNAME() {
        String topicName = "r_test_topic_23";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-23.csv";
        String queryName = "rTestQuery23";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1",null,1234567891));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null TIMESTAMP is read")
    public void testReadingNullTIMESTAMP() {
        String topicName = "r_test_topic_29";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-29.csv";
        String queryName = "rTestQuery29";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(1,"Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com",null));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with all null values is read")
    public void testReadingOnlyNullValues() {
        String topicName = "r_test_topic_30";
        String sourceCsvPath = "src/test/resources/ReadingFromKafka/r-test-data-30.csv";
        String queryName = "rTestQuery30";
        int rows = 1;
        List<Row> result = runReading(topicName, sourceCsvPath, queryName, rows);

        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(null,null,null,
                null,null,null,null,null));
        assertEquals(expectedResult, result);
    }
}
