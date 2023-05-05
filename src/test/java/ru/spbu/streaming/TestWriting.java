package ru.spbu.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;


import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWriting {
    public String url = "jdbc:postgresql://localhost:5432/postgres";
    public String user = "postgres";
    public String password = "postgres";
    String brokers = "localhost:29092";
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
    @DisplayName("App creates table if it doesn't exist")
    public void testTableNotExists() {
        String table = "test.table_1";
        String tableName = "table_1";
        String DLQTopicName = "w_test_topic_1";
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;e;abcdef1;www.publisher1.com;1234567891;" + rowInJson + ";1"};
        try {
            connection.createStatement().executeUpdate("DROP TABLE if exists " + table);
            runPipeline(table, tableData, DLQTopicName);
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT EXISTS (" +
                    " SELECT FROM pg_tables" +
                    " WHERE tablename  = ?);");
            preparedStatement.setString(1, tableName);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            assertEquals("t", resultSet.getString(1));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("App doesn't recreate table if it exists")
    public void testTableExists() {
        String table = "test.table_2";
        String DLQTopicName = "w_test_topic_2";
        String rowInJson = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","e","abcdef2","www.publisher2.com","1234567892");
        String[] tableData = {"2;Title 2;http://www.url2.com;Publisher 2;e;abcdef2;www.publisher2.com;1234567892;" + rowInJson + ";2"};
        try {
            String sqlTable = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891'))\n" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable);

            runPipeline(table, tableData, DLQTopicName);

            ResultSet resultSet = connection.createStatement().executeQuery("SELECT count(*) FROM ( SELECT * FROM postgres." + table + ") AS t");
            resultSet.next();
            int expectedRows = 2;
            int actualRows = resultSet.getInt(1);
            assertEquals(expectedRows, actualRows);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }


    @Test
    @DisplayName("App writes data at the end of existing table")
    public void testWritingToEnd() {
        String table = "test.table_3";
        String tempTable = "test.temp_table_3";
        String DLQTopicName = "w_test_topic_3";
        String rowInJson = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","e","abcdef2","www.publisher2.com","1234567892");
        String[] tableData = {"2;Title 2;http://www.url2.com,Publisher 2;e;abcdef2;www.publisher2.com;1234567892;" + rowInJson + ";2"};
        try {
            String sqlTable1 = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable1);
            String sqlTable2 = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')," +
                    "(2, 'Title 2', 'http://www.url2.com', 'Publisher 2', 'e', 'abcdef2', 'www.publisher2.com', '1234567892'))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable2);

            runPipeline(table, tableData, DLQTopicName);
            assertEquals(0, findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("App throws SQLException if titles are wrong")
    public void testWritingWrongTitlesTable() {
        String table = "test.table_4";
        String DLQTopicName = "w_test_topic_4";
        String rowInJson = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","e","abcdef2","www.publisher2.com","1234567892");
        String[] tableData = {"2;Title 2;http://www.url2.com;Publisher 2;e;abcdef2;www.publisher2.com;1234567892;" + rowInJson + ";2"};        try {
            String sqlTable = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, NOTTITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }

        boolean isException = false;
        try {
        runPipeline(table, tableData, DLQTopicName);
        } catch (SQLException e) {
            isException = true;
        }
        assertTrue(isException);
    }

    @Test
    @DisplayName("App throws SQLException if there are less columns")
    public void testWritingLessColumnsTable() {
        String table = "test.table_5";
        String DLQTopicName = "w_test_topic_5";
        String rowInJson = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","e","abcdef2","www.publisher2.com","1234567892");
        String[] tableData = {"2;Title 2;http://www.url2.com;Publisher 2;e;abcdef2;www.publisher2.com;1234567892;" + rowInJson + ";2"};        try {
            String sqlTable = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com')) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }

        boolean isException = false;
        try {
        runPipeline(table, tableData, DLQTopicName);
        } catch (SQLException e) {
            isException = true;
        }
        assertTrue(isException);
    }

    @Test
    @DisplayName("App throws SQLException if there are more columns")
    public void testWritingMoreColumnsTable() {
        String table = "test.table_6";
        String DLQTopicName = "w_test_topic_6";
        String rowInJson = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","e","abcdef2","www.publisher2.com","1234567892");
        String[] tableData = {"2;Title 2;http://www.url2.com;Publisher 2;e;abcdef2;www.publisher2.com;1234567892;" + rowInJson + ";2"};        try {
            String sqlTable = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP, EXTRA) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891', 'extra text')) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }

        boolean isException = false;
        try {
            runPipeline(table, tableData, DLQTopicName);
        } catch (SQLException e) {
            isException = true;
        }
        assertTrue(isException);
    }

    @Test
    @DisplayName("App throws SQLException if columns are in wrong order")
    public void testWritingWrongColumnOrderTable() {
        String table = "test.table_7";
        String DLQTopicName = "w_test_topic_7";
        String rowInJson = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","e","abcdef2","www.publisher2.com","1234567892");
        String[] tableData = {"2;Title 2;http://www.url2.com;Publisher 2;e;abcdef2;www.publisher2.com;1234567892;" + rowInJson + ";2"};
        try {
            String sqlTable = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, PUBLISHER, URL, TITLE, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Publisher 1' ,'http://www.url1.com', 'Title 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }

        boolean isException = false;
        try {
            runPipeline(table, tableData, DLQTopicName);
        } catch (SQLException e) {
            isException = true;
        }
        assertTrue(isException);
    }

    @Test
    @DisplayName("Empty strings are empty")
    public void testWritingEmptyStrValues() {
        String table = "test.table_8";
        String tempTable = "test.temp_table_8";
        String DLQTopicName = "w_test_topic_8";
        String rowInJson = createJsonInString("1", "","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {"1;;http://www.url1.com;Publisher 1;e;abcdef1;www.publisher1.com;1234567891;" + rowInJson + ";1"};
        try {
            String sqlTempTable = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, '', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            runPipeline(table, tableData, DLQTopicName);
            assertEquals(0, findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("Data are still correct after writing")
    public void testWritingCorrectData() {
        String table = "test.table_9";
        String tempTable = "test.temp_table_9";
        String DLQTopicName = "w_test_topic_9";
        String rowInJson1 = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String rowInJson2 = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892");
        String rowInJson3 = createJsonInString("3", "Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com","1234567893");
        String rowInJson4 = createJsonInString("4", "Title 4","http://www.url4.com",
                "Publisher 4","b","abcdef4","www.publisher4.com","1234567894");
        String rowInJson5 = createJsonInString("5", "Title 5","http://www.url5.com",
                "Publisher 5","m","abcdef5","www.publisher5.com","1234567895");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;e;abcdef1;www.publisher1.com;1234567891;" + rowInJson1 + ";1",
                "2;Title 2;http://www.url2.com;Publisher 2;t;abcdef2;www.publisher2.com;1234567892;" + rowInJson2 + ";2",
                "3;Title 3;http://www.url3.com;Publisher 3;b;abcdef3;www.publisher3.com;1234567893;" + rowInJson3 + ";3",
                "4;Title 4;http://www.url4.com;Publisher 4;b;abcdef4;www.publisher4.com;1234567894;" + rowInJson4 + ";4",
                "5;Title 5;http://www.url5.com;Publisher 5;m;abcdef5;www.publisher5.com;1234567895;" + rowInJson5 + ";5"};
        try {
            String sqlTempTable = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')," +
                    "(2, 'Title 2', 'http://www.url2.com', 'Publisher 2', 't', 'abcdef2', 'www.publisher2.com', '1234567892')," +
                    "(3, 'Title 3', 'http://www.url3.com', 'Publisher 3', 'b', 'abcdef3', 'www.publisher3.com', '1234567893')," +
                    "(4, 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', '1234567894')," +
                    "(5, 'Title 5', 'http://www.url5.com', 'Publisher 5', 'm', 'abcdef5', 'www.publisher5.com', '1234567895'))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            runPipeline(table, tableData, DLQTopicName);
            assertEquals(0, findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }


    @Test
    @DisplayName("Only rows with correct ID are written to DB")
    public void testWritingIncorrectIDToDB() {
        String table = "test.table_10";
        String tempTable = "test.temp_table_10";
        String DLQTopicName = "w_test_topic_10";
        String rowInJson1 = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String rowInJson2 = createJsonInString("incorrectID1", "Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892");
        String rowInJson3 = createJsonInString("incorrectID2", "Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com","1234567893");
        String rowInJson4 = createJsonInString("4", "Title 4","http://www.url4.com",
                "Publisher 4","b","abcdef4","www.publisher4.com","1234567894");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;e;abcdef1;www.publisher1.com;1234567891;" + rowInJson1 + ";1",
                "incorrectID1;Title 2;http://www.url2.com;Publisher 2;t;abcdef2;www.publisher2.com;1234567892;" + rowInJson2 + ";null",
                "incorrectID2;Title 3;http://www.url3.com;Publisher 3;b;abcdef3;www.publisher3.com;1234567893;" + rowInJson3 + ";null",
                "4;Title 4;http://www.url4.com;Publisher 4;b;abcdef4;www.publisher4.com;1234567894;" + rowInJson4 + ";4"};
        try {
            String sqlTempTable = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')," +
                    "(4, 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', '1234567894'))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            runPipeline(table, tableData, DLQTopicName);
            assertEquals(0, findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }


    @Test
    @DisplayName("Only rows with correct category are written to DB")
    public void testWritingIncorrectCATEGORYToDB() {
        String table = "test.table_11";
        String tempTable = "test.temp_table_11";
        String DLQTopicName = "w_test_topic_11";
        String rowInJson1 = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String rowInJson2 = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","x","abcdef2","www.publisher2.com","1234567892");
        String rowInJson3 = createJsonInString("3", "Title 3","http://www.url3.com",
                "Publisher 3","y","abcdef3","www.publisher3.com","1234567893");
        String rowInJson4 = createJsonInString("4", "Title 4","http://www.url4.com",
                "Publisher 4","b","abcdef4","www.publisher4.com","1234567894");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;e;abcdef1;www.publisher1.com;1234567891;" + rowInJson1 + ";1",
                "1;Title 2;http://www.url2.com;Publisher 2;x;abcdef2;www.publisher2.com;1234567892;" + rowInJson2 + ";2",
                "2;Title 3;http://www.url3.com;Publisher 3;y;abcdef3;www.publisher3.com;1234567893;" + rowInJson3 + ";3",
                "4;Title 4;http://www.url4.com;Publisher 4;b;abcdef4;www.publisher4.com;1234567894;" + rowInJson4 + ";4"};
        try {
            String sqlTempTable = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', '1234567891')," +
                    "(4, 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', '1234567894'))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            runPipeline(table, tableData, DLQTopicName);
            assertEquals(0, findDifferentRowsNum(table, tempTable));
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }


    @Test
    @DisplayName("Row with string ID is written to DLQ")
    public void testWritingStringIDToDLQ() {
        String table = "test.table_12";
        String DLQTopicName = "w_test_topic_12";
        String queryName = "wTestQuery12";
        String rowInJson = createJsonInString("incorrectID", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {"incorrectID,Title 1,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891," + rowInJson + ",null"};
        int rows = 1;
        List<Row> result = runPipelineWithDLQ(table, tableData, queryName, DLQTopicName, rows);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(rowInJson));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with incorrect category 'x' is written to DLQ")
    public void testWritingIncorrectCATEGORYToDLQ() throws TimeoutException {
        String table = "test.table_13";
        String DLQTopicName = "w_test_topic_13";
        String queryName = "wTestQuery13";
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","x","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;x;abcdef1;www.publisher1.com;1234567891;" + rowInJson + ";1"};
        int rows = 1;
        List<Row> result = runPipelineWithDLQ(table, tableData, queryName, DLQTopicName, rows);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(rowInJson));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with incorrect category '1' is written to DLQ")
    public void testWritingIncorrectIntCATEGORYToDLQ() throws TimeoutException {
        String table = "test.table_14";
        String DLQTopicName = "w_test_topic_14";
        String queryName = "wTestQuery14";
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","1","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;x;abcdef1;www.publisher1.com;1234567891;" + rowInJson + ";1"};
        int rows = 1;
        List<Row> result = runPipelineWithDLQ(table, tableData, queryName, DLQTopicName, rows);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(rowInJson));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Rows with incorrect ID are written to DLQ")
    public void testWritingIncorrectIDToDLQ() {
        String table = "test.table_15";
        String DLQTopicName = "w_test_topic_15";
        String queryName = "wTestQuery15";
        String rowInJson1 = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String rowInJson2 = createJsonInString("incorrectID1", "Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892");
        String rowInJson3 = createJsonInString("incorrectID2", "Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com","1234567893");
        String rowInJson4 = createJsonInString("4", "Title 4","http://www.url4.com",
                "Publisher 4","b","abcdef4","www.publisher4.com","1234567894");
        String[] tableData = {"1;Title 1;http://www.url1.com;Publisher 1;e;abcdef1;www.publisher1.com;1234567891;" + rowInJson1 + ";1",
                "incorrectID1;Title 2;http://www.url2.com;Publisher 2;t;abcdef2;www.publisher2.com;1234567892;" + rowInJson2 + ";null",
                "incorrectID2;Title 3;http://www.url3.com;Publisher 3;b;abcdef3;www.publisher3.com;1234567893;" + rowInJson3 + ";null",
                "4;Title 4;http://www.url4.com;Publisher 4;b;abcdef4;www.publisher4.com;1234567894;" + rowInJson4 + ";4"};
        int rows = 2;
        List<Row> result = runPipelineWithDLQ(table, tableData, queryName, DLQTopicName, rows);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create(rowInJson2));
        expectedResult.add(RowFactory.create(rowInJson3));
        assertEquals(expectedResult, result);
    }


    @Test
    @DisplayName("App throws SQLException if table is deleted")
    public void testDeleteTable() {
        String table = "test.table_16";
        String DLQTopicName = "w_test_topic_16";
        String SQLrequest = "DROP TABLE if exists test.nf_w_table_1";
        boolean isException = changeTable(table, SQLrequest, DLQTopicName);
        assertTrue(isException);
    }


    @Test
    @DisplayName("App throws SQLException if column is renamed")
    public void testRenameColumn(){
        String table = "test.table_17";
        String DLQTopicName = "w_test_topic_17";
        String SQLrequest = "ALTER TABLE test.nf_w_table_2 RENAME COLUMN \"ID\" TO \"NEW_ID\";";
        boolean isException = changeTable(table, SQLrequest, DLQTopicName);
        assertTrue(isException);
    }

    @Test
    @DisplayName("App throws SQLException if column is deleted")
    public void testDeleteColumn(){
        String table = "test.table_18";
        String DLQTopicName = "w_test_topic_18";
        String SQLrequest = "ALTER TABLE test.nf_w_table_3 DROP COLUMN \"ID\";";
        boolean isException = changeTable(table, SQLrequest, DLQTopicName);
        assertTrue(isException);
    }

    @Test
    @DisplayName("App throws SQLException if new column added")
    public void testAddColumn(){
        String table = "test.table_19";
        String DLQTopicName = "w_test_topic_19";
        String SQLrequest = "ALTER TABLE test.nf_w_table_4 ADD COLUMN \"NEW_COLUMN\" VARCHAR;";
        boolean isException = changeTable(table, SQLrequest, DLQTopicName);
        assertTrue(isException);
    }

    @AfterAll
    public void closeConnection() {
        try {
            for (int i = 1; i <= 14; i++) {
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_1");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_2");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_3");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.temp_table_3");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_4");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_5");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_6");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_7");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_8");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.temp_table_8");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_9");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.temp_table_9");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_10");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.temp_table_10");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_11");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.temp_table_11");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_12");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_13");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_14");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_15");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_16");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_17");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_18");
                connection.createStatement().executeUpdate("DROP TABLE if exists test.table_19");

            }
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

    private Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }



    private Dataset<Row> createTestStreamingDataFrame(SparkSession spark, String[] tableData) {
        Option<Object> numPartitions = Option.apply(1);
        MemoryStream<String> testStream = new MemoryStream<>(1, spark.sqlContext(), numPartitions, Encoders.STRING());
        testStream.addData(convertListToSeq(Arrays.asList(tableData)));
        return testStream.toDF().selectExpr(
                "cast(split(value,'[,]')[0] as string) as ID_STR",
                "cast(split(value,'[;]')[1] as string) as TITLE",
                "cast(split(value,'[;]')[2] as string) as URL",
                "cast(split(value,'[;]')[3] as string) as PUBLISHER",
                "cast(split(value,'[;]')[4] as string) as CATEGORY",
                "cast(split(value,'[;]')[5] as string) as STORY",
                "cast(split(value,'[;]')[6] as string) as HOSTNAME",
                "cast(split(value,'[;]')[7] as string) as TIMESTAMP",
                "cast(split(value,'[;]')[8] as string) as original_value",
                "cast(split(value,'[;]')[9] as long) as ID");
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



    private List<Row> runPipelineWithDLQ(String table, String[] tableData, String queryName, String DLQTopicName, int rows) {
        StreamingPipeline streamingPipeline = new StreamingPipeline();
        SparkSession spark = streamingPipeline.getSparkSession();
        Thread t1 = runReadingFromDLQ(spark, queryName, DLQTopicName);
        Thread t2 = runWriting(streamingPipeline, spark, table, tableData, DLQTopicName);
        t1.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t2.start();

        int currentRows = 0;
        long start = System.currentTimeMillis();
        long end = start + 30 * 1000;
        while (currentRows < rows && System.currentTimeMillis() < end) {
            currentRows = spark.sql("select * from " + queryName).collectAsList().size();
        }
        List<Row> result = spark.sql("select * from " + queryName).collectAsList();
        t1.interrupt();
        t2.interrupt();
        return result;
    }

    private Thread runWriting(StreamingPipeline streamingPipeline, SparkSession spark,
                              String table, String[] tableData, String DLQTopicName) {
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    Dataset<Row> dataset = createTestStreamingDataFrame(spark, tableData);
                    streamingPipeline.writeStreamingDataset(user, password, url, table, dataset, brokers, DLQTopicName);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
    }

    private void runPipeline(String table, String[] tableData, String DLQTopicName) {
        StreamingPipeline streamingPipeline = new StreamingPipeline();
        SparkSession spark = streamingPipeline.getSparkSession();
        Thread t = runWriting(streamingPipeline, spark, table, tableData, DLQTopicName);
        t.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t.interrupt();
    }

    private boolean changeTable(String table, String SQLrequest, String DLQTopicName) {
        boolean isException = false;
        StreamingPipeline streamingPipeline = new StreamingPipeline();
        SparkSession spark = streamingPipeline.getSparkSession();
        String[] tableData = {"1,Title 1,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891"};
        Dataset<Row> dataset = createTestStreamingDataFrame(spark, tableData);
        Thread t = createChangingThread(SQLrequest);
        t.start();
        try {
            StreamingQuery streamingQuery = streamingPipeline.writeStreamingDataset(user, password, url, table, dataset, brokers, DLQTopicName);
            streamingQuery.awaitTermination(25000);
        } catch (SQLException e) {
            isException = true;
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        return isException;
    }

    private Thread createChangingThread(String SQLrequest) {
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    Thread.sleep(15000);
                    connection.createStatement().executeUpdate(SQLrequest);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
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
}
