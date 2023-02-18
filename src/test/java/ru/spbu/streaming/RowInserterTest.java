package ru.spbu.streaming;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowInserterTest {

    @Test
    public void testInsertToPostgres() throws SQLException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("src/test/resources/uci-news-aggregator.csv");

        RowInserter sut = new RowInserter();

//        Row[] collect = df.collect();
//        List<Row> list = new ArrayList<>();
//        for (int i = 0; i < collect.length; i++) {
//            list.add(collect[i]);
//        }
//        sut.processRows(list.iterator());



    }
}

