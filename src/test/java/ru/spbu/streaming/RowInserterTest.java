package ru.spbu.streaming;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import ru.spbu.streaming.RowInserter;
import ru.spbu.streaming.WriteToKafka;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class RowInserterTest {

    public String url = "jdbc:postgresql://localhost:5432/postgres";
    public String user = "postgres";
    public String password = "postgres";
    public Properties connectionProperties;
    public Connection connection;


    public Thread runRowInserter(String table, String topicName) {
        String brokers = "localhost:29092";
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    RowInserter.rowInserter(brokers, topicName, user, password, url, table);
                } catch (StreamingQueryException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
    }

    public void runPipeline(String table, String topicName, String sourceCsvPath, int rows) {
        Thread t = runRowInserter(table, topicName);
        t.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        WriteToKafka.writeKafka(sourceCsvPath, topicName);
        String fullTableName = "postgres." + table;
        int currentRows = 0;
        long start = System.currentTimeMillis();
        long end = start + 30 * 1000;
        while(currentRows < rows && System.currentTimeMillis() < end) {
            try (ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from " + fullTableName)) {
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
    public void testCorrectDataset() {
        String topicName = "test_1";
        String table = "test.table_1";
        String sourceCsvPath = "src/test/resources/test-data-1.csv";
        int rows = 5;

        runPipeline(table, topicName, sourceCsvPath, rows);

        try {
            String sqlTempTable = "CREATE TABLE temp_table_1 AS\n" +
                    "WITH vals (ID, TITLE, UTL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES \n" +
                    "\t('1', 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891'),\n" +
                    "\t('2', 'Title 2', 'http://www.url2.com', 'Publisher 2', 't', 'abcdef2', 'www.publisher2.com', '1234567892'),\n" +
                    "\t('3', 'Title 3', 'http://www.url3.com', 'Publisher 3', 'b', 'abcdef3', 'www.publisher3.com', '1234567893'),\n" +
                    "\t('4', 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', '1234567894'),\n" +
                    "\t('5', 'Title 5', 'http://www.url5.com', 'Publisher 5', 'm', 'abcdef5', 'www.publisher5.com', '1234567895'))\n" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from ( select * from postgres.public.temp_table_1" +
                    " intersect" +
                    " select * from postgres.test.table_1) as t");
            resultSet.next();
            assertEquals(resultSet.getInt(1), 5);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }

    }

    @AfterAll
    public void closeConnection() {
        try {
            connection.createStatement().executeUpdate("DROP TABLE if exists public.temp_table_1");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.table_1");
            connection.createStatement().executeUpdate("DROP TABLE if exists test.table_2");

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
}

