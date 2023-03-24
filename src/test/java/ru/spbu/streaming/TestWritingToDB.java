package ru.spbu.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;


import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWritingToDB {
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
    @DisplayName("App creates table if it doesn't exist")
    public void testTableNotExists() {
        String table = "test.table_1";
        String tableName = "table_1";
        String[] tableData = {"1,Title 1,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891"};
        try {
            connection.createStatement().executeUpdate("DROP TABLE if exists " + table);
            runPipeline(table, tableData);

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
        String[] tableData = {"2,Title 2,http://www.url2.com,Publisher 2,e,abcdef2,www.publisher2.com,1234567892"};
        try {
            String sqlTable = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES \n" +
                    "\t(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891))\n" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable);

            runPipeline(table, tableData);

            ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from ( select * from postgres." + table +
                    " intersect" +
                    " select * from postgres." + table + ") as t");
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
        String[] tableData = {"2,Title 2,http://www.url2.com,Publisher 2,e,abcdef2,www.publisher2.com,1234567892"};
        try {
            String sqlTable1 = "CREATE TABLE " + table + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable1);
            String sqlTable2 = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)," +
                    "(2, 'Title 2', 'http://www.url2.com', 'Publisher 2', 'e', 'abcdef2', 'www.publisher2.com', 1234567892))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTable2);

            runPipeline(table, tableData);

            ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from ( select * from postgres." + table +
                    " intersect" +
                    " select * from postgres." + tempTable + ") as t");
            resultSet.next();
            int expectedRows = 2;
            int actualRows = resultSet.getInt(1);
            assertEquals(expectedRows, actualRows);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("App throws exception (which one??) if titles are wrong")
    public void testWritingWrongTitlesTable() {
        assertThrows(RuntimeException.class,
                ()->{
                    String table = "test.table_4";
                    String[] tableData = {"2,Title 2,http://www.url2.com,Publisher 2,e,abcdef2,www.publisher2.com,1234567892"};
                    try {
                        String sqlTable = "CREATE TABLE " + table + " AS " +
                                "WITH vals (ID, NOTTITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                                "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)) " +
                                "SELECT * FROM vals;";
                        connection.createStatement().executeUpdate(sqlTable);

                        runPipeline(table, tableData);
                    } catch (SQLException e){
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    @DisplayName("App throws exception (which one??) if there are less columns")
    public void testWritingLessColumnsTable() {
        assertThrows(RuntimeException.class,
                ()->{
                    String table = "test.table_5";
                    String[] tableData = {"2,Title 2,http://www.url2.com,Publisher 2,e,abcdef2,www.publisher2.com,1234567892"};
                    try {
                        String sqlTable = "CREATE TABLE " + table + " AS " +
                                "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME) AS (VALUES " +
                                "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com')) " +
                                "SELECT * FROM vals;";
                        connection.createStatement().executeUpdate(sqlTable);

                        runPipeline(table, tableData);
                    } catch (SQLException e){
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    @DisplayName("App throws exception (which one??) if there are more columns")
    public void testWritingMoreColumnsTable() {
        assertThrows(RuntimeException.class,
                ()->{
                    String table = "test.table_6";
                    String[] tableData = {"2,Title 2,http://www.url2.com,Publisher 2,e,abcdef2,www.publisher2.com,1234567892"};
                    try {
                        String sqlTable = "CREATE TABLE " + table + " AS " +
                                "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP, EXTRA) AS (VALUES " +
                                "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891, 'extra text')) " +
                                "SELECT * FROM vals;";
                        connection.createStatement().executeUpdate(sqlTable);

                        runPipeline(table, tableData);
                    } catch (SQLException e){
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    @DisplayName("App throws exception (which one??) if columns are in wrong order")
    public void testWritingWrongColumnOrderTable() {
        assertThrows(RuntimeException.class,
                ()->{
                    String table = "test.table_7";
                    String[] tableData = {"2,Title 2,http://www.url2.com,Publisher 2,e,abcdef2,www.publisher2.com,1234567892"};
                    try {
                        String sqlTable = "CREATE TABLE " + table + " AS " +
                                "WITH vals (ID, PUBLISHER, URL, TITLE, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                                "(1, 'Publisher 1' ,'http://www.url1.com', 'Title 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)) " +
                                "SELECT * FROM vals;";
                        connection.createStatement().executeUpdate(sqlTable);

                        runPipeline(table, tableData);
                    } catch (SQLException e){
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    @DisplayName("Empty strings are empty")
    public void testWritingEmptyStrValues() {
        String table = "test.table_8";
        String tempTable = "test.temp_table_8";
        String[] tableData = {"1,,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891"};
        try {
            String sqlTempTable = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, '', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)) " +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            runPipeline(table, tableData);

            ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from ( select * from postgres." + table +
                    " intersect" +
                    " select * from postgres." + tempTable + ") as t");
            resultSet.next();
            int expectedRows = 1;
            int actualRows = resultSet.getInt(1);
            assertEquals(expectedRows, actualRows);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("Data are still correct after writing")
    public void testWrittenData() {
        String table = "test.table_9";
        String tempTable = "test.temp_table_9";
        String[] tableData = {"1,Title 1,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891",
                "2,Title 2,http://www.url2.com,Publisher 2,t,abcdef2,www.publisher2.com,1234567892",
                "3,Title 3,http://www.url3.com,Publisher 3,b,abcdef3,www.publisher3.com,1234567893",
                "4,Title 4,http://www.url4.com,Publisher 4,b,abcdef4,www.publisher4.com,1234567894",
                "5,Title 5,http://www.url5.com,Publisher 5,m,abcdef5,www.publisher5.com,1234567895"};
        try {
            String sqlTempTable = "CREATE TABLE " + tempTable + " AS " +
                    "WITH vals (ID, TITLE, URL, PUBLISHER, CATEGORY, STORY, HOSTNAME, TIMESTAMP) AS (VALUES " +
                    "(1, 'Title 1', 'http://www.url1.com', 'Publisher 1', 'e', 'abcdef1', 'www.publisher1.com', 1234567891)," +
                    "(2, 'Title 2', 'http://www.url2.com', 'Publisher 2', 't', 'abcdef2', 'www.publisher2.com', 1234567892)," +
                    "(3, 'Title 3', 'http://www.url3.com', 'Publisher 3', 'b', 'abcdef3', 'www.publisher3.com', 1234567893)," +
                    "(4, 'Title 4', 'http://www.url4.com', 'Publisher 4', 'b', 'abcdef4', 'www.publisher4.com', 1234567894)," +
                    "(5, 'Title 5', 'http://www.url5.com', 'Publisher 5', 'm', 'abcdef5', 'www.publisher5.com', 1234567895))" +
                    "SELECT * FROM vals;";
            connection.createStatement().executeUpdate(sqlTempTable);

            runPipeline(table, tableData);

            ResultSet resultSet = connection.createStatement().executeQuery("select count(*) from ( select * from postgres." + table +
                    " intersect" +
                    " select * from postgres." + tempTable + ") as t");
            resultSet.next();
            int expectedRows = 5;
            int actualRows = resultSet.getInt(1);
            assertEquals(expectedRows, actualRows);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public void closeConnection() {
        try {
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

    private Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    private Dataset<Row> createTestStreamingDataFrame(SparkSession spark, String[] tableData) {
        scala.Option<Object> numPartitions = scala.Option.apply(null);
        MemoryStream<String> testStream = new MemoryStream<>(1, spark.sqlContext(), numPartitions, Encoders.STRING());
        testStream.addData(convertListToSeq(Arrays.asList(tableData)));
        return testStream .toDF().selectExpr(
                "cast(split(value,'[,]')[0] as int) as ID",
                "cast(split(value,'[,]')[1] as string) as TITLE",
                "cast(split(value,'[,]')[2] as string) as URL",
                "cast(split(value,'[,]')[3] as string) as PUBLISHER",
                "cast(split(value,'[,]')[4] as string) as CATEGORY",
                "cast(split(value,'[,]')[5] as string) as STORY",
                "cast(split(value,'[,]')[6] as string) as HOSTNAME",
                "cast(split(value,'[,]')[7] as int) as TIMESTAMP");
    }


    private Thread runRowInserter(String table, String[] tableData) {
        Thread t = new Thread(() -> {
            if (!Thread.interrupted()) {
                try {
                    RowInserter rowInserter = new RowInserter();
                    SparkSession spark = rowInserter.getSparkSession();
                    Dataset<Row> dataset = createTestStreamingDataFrame(spark, tableData);
                    StreamingQuery streamingQuery = rowInserter.writeStreamingDataset(user, password, url, table, dataset);
                    streamingQuery.awaitTermination();
                } catch (StreamingQueryException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return t;
    }

    private void runPipeline(String table, String[] tableData) {
        Thread t = runRowInserter(table, tableData);
        t.start();
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t.interrupt();
    }

}
