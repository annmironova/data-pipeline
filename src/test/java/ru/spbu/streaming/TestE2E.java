package ru.spbu.streaming;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.logging.log4j.util.PropertiesUtil.getProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestE2E {

    String brokers = "localhost:29092";
    String url = "jdbc:postgresql://localhost:5432/postgres";
    String user = "postgres";
    String password = "postgres";
    Properties connectionProperties;
    Connection connection;

    @BeforeAll
    public void setConnection() {
        connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        try {
            connection = DriverManager.getConnection(url, connectionProperties);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    @DisplayName("E2E test to check that correct data is written to DB")
    public void testDataIsWritten() {
        String topicName = "e2e_test_topic_1";
        String DLQtopicName = "e2e_test_dlqtopic_1";
        String table = "test.e2e_table_1";
        String sourceCsvPath = "src/test/resources/E2E/e2e-test-data-1.csv";
        int rows = 5;

        runPipeline(table, topicName, DLQtopicName, sourceCsvPath, rows);

        try {
            String SQLrequest2 = "SELECT COUNT(*) FROM (SELECT * FROM " + table + ") as t;";
            ResultSet resultSet = connection.createStatement().executeQuery(SQLrequest2);
            resultSet.next();
            int actualRows = resultSet.getInt(1);
            assertTrue(actualRows >= rows);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("E2E test with correct data to check written data")
    public void testCorrectDataset() {
        String topicName = "e2e_test_topic_2";
        String DLQtopicName = "e2e_test_dlqtopic_2";
        String table = "test.e2e_table_2";
        String tempTable = "test.e2e_temp_table_2";
        String sourceCsvPath = "src/test/resources/E2E/e2e-test-data-2.csv";
        int rows = 5;

        runPipeline(table, topicName, DLQtopicName, sourceCsvPath, rows);

        try {
            String SQLrequest = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')," +
                    "(2, 'Title 2', 'http://www.url2.com', 'Publisher 2', 't', 'abcdef2', 'www.publisher2.com', '1234567892')," +
                    "(3, 'Title 3', 'http://www.url3.com', 'Publisher 3', 'b', 'abcdef3', 'www.publisher3.com', '1234567893')," +
                    "(4, 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', '1234567894')," +
                    "(5, 'Title 5', 'http://www.url5.com', 'Publisher 5', 'm', 'abcdef5', 'www.publisher5.com', '1234567895'))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(SQLrequest);
            assertEquals(0,  findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("E2E test with correct and incorrect data to check written to DB data")
    public void testCorrectDataIsWrittenToDB() {
        String topicName = "e2e_test_topic_3";
        String DLQtopicName = "e2e_test_dlqtopic_3";
        String table = "test.e2e_table_3";
        String tempTable = "test.e2e_temp_table_3";
        String sourceCsvPath = "src/test/resources/E2E/e2e-test-data-3.csv";
        int rows = 3;

        runPipeline(table, topicName, DLQtopicName, sourceCsvPath, rows);

        try {
            String SQLrequest = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')," +
                    "(3, 'Title 3', 'http://www.url3.com', 'Publisher 3', 'b', 'abcdef3', 'www.publisher3.com', '1234567893')," +
                    "(5, 'Title 5', 'http://www.url5.com', 'Publisher 5', 'm', 'abcdef5', 'www.publisher5.com', '1234567895'))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(SQLrequest);

            assertEquals(0,  findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }


    @Test
    @DisplayName("E2E test with correct and incorrect data to check written to DLQ data")
    public void testIncorrectDataIsWrittenToDB() {
        String topicName = "e2e_test_topic_4";
        String DLQtopicName = "e2e_test_dlqtopic_4";
        String table = "test.e2e_table_4";
        String queryName = "e2eTestQuery4";
        String sourceCsvPath = "src/test/resources/E2E/e2e-test-data-4.csv";
        String rowInJson2 = createJsonInString("incorrectID2", "Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892");
        String rowInJson4 = createJsonInString("incorrectID4", "Title 4","http://www.url4.com",
                "Publisher 4","b","abcdef4","www.publisher4.com","1234567894");
        int correctRows = 3;
        SparkSession spark = SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();
        Thread t = runReadingFromDLQ(spark, queryName, DLQtopicName);
        t.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        runPipeline(table, topicName, DLQtopicName, sourceCsvPath, correctRows);
        List<Row> result = spark.sql("select * from " + queryName).collectAsList();
        t.interrupt();
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(rowInJson2));
        expectedResult.add(RowFactory.create(rowInJson4));
        assertEquals(expectedResult, result);
    }

    @AfterAll
    public void closeConnection() {
        try {
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_table_1");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_table_2");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_temp_table_2");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_table_3");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_temp_table_3");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_table_4");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) { connection.close();}
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private Thread runRowInserter(String table, String topicName, String DLQtopicName) {
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    StreamingPipeline.rowInserter(brokers, topicName, DLQtopicName, user, password, url, table);
                } catch (StreamingQueryException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
    }

    private void runPipeline(String table, String topicName, String DLQtopicName, String sourceCsvPath, int rows) {
        Thread t = runRowInserter(table, topicName, DLQtopicName);
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
            try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
                resultSet.next();
                currentRows = resultSet.getInt(1);
                Thread.sleep(5000);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (currentRows < rows) {
            throw new RuntimeException("Expected more rows");
        }
        t.interrupt();
    }

    private int findDifferentRowsNum(String tableName, String tempTableName) {
        try {
            ResultSet resultSet = connection.createStatement()
                    .executeQuery("SELECT count(*) FROM (SELECT * FROM  postgres." + tableName +
                            " UNION SELECT * FROM postgres." + tempTableName + " EXCEPT SELECT * FROM postgres." + tableName +
                            " INTERSECT SELECT * FROM postgres." + tempTableName + ") as t");
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String createJsonInString(String ID, String TITLE, String URL, String PUBLISHER,
                                      String CATEGORY, String STORY, String HOSTNAME, String TIMESTAMP) {
        String row = "{\"ID\":\"" + ID + "\"," +
                "\"TITLE\":\"" + TITLE + "\"," +
                "\"URL\":\"" + URL + "\"," +
                "\"PUBLISHER\":\"" + PUBLISHER + "\"," +
                "\"CATEGORY\":\"" + CATEGORY + "\"," +
                "\"STORY\":\"" + STORY + "\"," +
                "\"HOSTNAME\":\"" + HOSTNAME + "\"," +
                "\"TIMESTAMP\":\"" + TIMESTAMP + "\"}";
        return row;
    }

    private Thread runReadingFromDLQ(SparkSession spark, String queryName, String DLQTopicName){
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                Dataset<Row> dataset = spark.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", brokers)
                        .option("subscribe", DLQTopicName)
                        .load();
                Dataset<Row> datasetTransformed = dataset
                        .selectExpr("CAST(value as string)");
                StreamingQuery streamingQuery = null;
                try {
                    streamingQuery = datasetTransformed
                            .writeStream()
                            .format("memory")
                            .queryName(queryName)
                            .outputMode("append")
                            .start();
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
                try {
                    streamingQuery.awaitTermination();
                } catch (StreamingQueryException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
    }
}

