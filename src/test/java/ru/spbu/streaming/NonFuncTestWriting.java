package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;


// ALL TIMEOUTEXCEPTIONS TO SQLEXCEPTIONS
@TestInstance(PER_CLASS)
public class NonFuncTestWriting {
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
    public void testDeleteTable() {
        String table = "test.nf_w_table_1";
        String SQLrequest = "DROP TABLE if exists test.nf_w_table_1";
        boolean isException = changeTable(table, SQLrequest);
        assertTrue(isException);
    }


    @Test
    public void testRenameColumn(){
        String table = "test.nf_w_table_2";
        String SQLrequest = "ALTER TABLE test.nf_w_table_2 RENAME COLUMN \"ID\" TO \"NEW_ID\";";
        boolean isException = changeTable(table, SQLrequest);
        assertTrue(isException);
    }

    @Test
    public void testDeleteColumn(){
        String table = "test.nf_w_table_3";
        String SQLrequest = "ALTER TABLE test.nf_w_table_3 DROP COLUMN \"ID\";";
        boolean isException = changeTable(table, SQLrequest);
        assertTrue(isException);
    }

    @Test
    public void testAddColumn(){
        String table = "test.nf_w_table_4";
        String SQLrequest = "ALTER TABLE test.nf_w_table_4 ADD COLUMN \"NEW_COLUMN\" VARCHAR;";
        boolean isException = changeTable(table, SQLrequest);
        assertTrue(isException);
    }

    private boolean changeTable(String table, String SQLrequest) {
        boolean isException = false;
        RowInserter rowInserter = new RowInserter();
        SparkSession spark = rowInserter.getSparkSession();
        String[] tableData = {"1,Title 1,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891"};
        Dataset<Row> dataset = createTestStreamingDataFrame(spark, tableData);
        Thread t = createChangingThread(SQLrequest);
        t.start();
        try {
            StreamingQuery streamingQuery = rowInserter.writeStreamingDataset(user, password, url, table, dataset);
            streamingQuery.awaitTermination(25000);
        } catch (TimeoutException e) {
            isException = true;
        } catch (StreamingQueryException e) {
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

    private Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    private Dataset<Row> createTestStreamingDataFrame(SparkSession spark, String[] tableData) {
        Option<Object> numPartitions = Option.apply(1);
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
}