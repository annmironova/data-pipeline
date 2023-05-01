package ru.spbu.streaming;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestE2E {

    public String url = "jdbc:postgresql://localhost:5432/postgres";
    public String user = "postgres";
    public String password = "postgres";
    public Properties connectionProperties;
    public Connection connection;

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
    @DisplayName("E2E test with correct data to check written data")
    public void testCorrectDataset() {
        String topicName = "e2e_test_topic_1";
        String table = "test.e2e_table_1";
        String tempTable = "test.e2e_temp_table_1";
        String sourceCsvPath = "src/test/resources/test-data-1.csv";
        int rows = 5;

        runPipeline(table, topicName, sourceCsvPath, rows);

        try {
            String SQLrequest1 = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)," +
                    "(2, 'Title 2', 'http://www.url2.com', 'Publisher 2', 't', 'abcdef2', 'www.publisher2.com', 1234567892)," +
                    "(3, 'Title 3', 'http://www.url3.com', 'Publisher 3', 'b', 'abcdef3', 'www.publisher3.com', 1234567893)," +
                    "(4, 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', 1234567894)," +
                    "(5, 'Title 5', 'http://www.url5.com', 'Publisher 5', 'm', 'abcdef5', 'www.publisher5.com', 1234567895))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(SQLrequest1);

            String SQLrequest2 = "SELECT COUNT(*) FROM ( SELECT * FROM " + tempTable +
                    " INTERSECT SELECT * FROM " + table + ") as t;";
            ResultSet resultSet = connection.createStatement().executeQuery(SQLrequest2);
            resultSet.next();
            int actualRows = resultSet.getInt(1);
            assertEquals(rows, actualRows);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public void closeConnection() {
        try {
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_table_1");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.e2e_temp_table_1");
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


    private Thread runRowInserter(String table, String topicName) {
        String brokers = "localhost:29092";
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    StreamingPipeline.rowInserter(brokers, topicName, user, password, url, table);
                } catch (StreamingQueryException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
    }

    private void runPipeline(String table, String topicName, String sourceCsvPath, int rows) {
        Thread t = runRowInserter(table, topicName);
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


}

