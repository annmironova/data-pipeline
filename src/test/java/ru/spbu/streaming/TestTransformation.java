package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTransformation {

    @NotNull
    private StructType getSchema() {
        StructType schema = new StructType(new StructField[]{
                new StructField("ID", DataTypes.StringType, false, Metadata.empty()),
                new StructField("TITLE", DataTypes.StringType, false, Metadata.empty()),
                new StructField("URL", DataTypes.StringType, false, Metadata.empty()),
                new StructField("PUBLISHER", DataTypes.StringType, false, Metadata.empty()),
                new StructField("CATEGORY", DataTypes.StringType, false, Metadata.empty()),
                new StructField("STORY", DataTypes.StringType, false, Metadata.empty()),
                new StructField("HOSTNAME", DataTypes.StringType, false, Metadata.empty()),
                new StructField("TIMESTAMP", DataTypes.StringType, false, Metadata.empty())
        });
        return schema;
    }

    private Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    //actually failed because of null
    @Test
    public void testTransform() throws TimeoutException {
        RowInserter rowInserter = new RowInserter();
        SparkSession spark = rowInserter.getSparkSession();
        Option<Object> numPartitions = Option.apply(1);

        MemoryStream<String> testStream = new MemoryStream<>(1, spark.sqlContext(), numPartitions, Encoders.STRING());
        //String jsonPath = "src/test/resources/test-data.json";
        //StructType structType = getSchema();
        //Dataset<String> personEventDataset = spark.read().json(jsonPath).as(Encoders.STRING());
        //testStream.addData(JavaConverters.asScalaIteratorConverter(personEventDataset.collectAsList().iterator()).asScala().toSeq());
        String[] tableData = {"ID,TITLE,URL,PUBLISHER,CATEGORY,STORY,HOSTNAME,TIMESTAMP","1,Title 1,http://www.url1.com,Publisher 1,e,abcdef1,www.publisher1.com,1234567891"};
        testStream.addData(convertListToSeq(Arrays.asList(tableData)));
        /*testStream.toDF().selectExpr(
                "cast(split(value,'[,]')[0] as int) as ID",
                "cast(split(value,'[,]')[1] as string) as TITLE",
                "cast(split(value,'[,]')[2] as string) as URL",
                "cast(split(value,'[,]')[3] as string) as PUBLISHER",
                "cast(split(value,'[,]')[4] as string) as CATEGORY",
                "cast(split(value,'[,]')[5] as string) as STORY",
                "cast(split(value,'[,]')[6] as string) as HOSTNAME",
                "cast(split(value,'[,]')[7] as int) as TIMESTAMP");*/
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

       // assertEquals(1, result.size());
        assertEquals(RowFactory.create("India", "marketing", "bella"), result.get(0));


    }
}
