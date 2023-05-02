package ru.spbu.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTransformation {

    @Test
    @DisplayName("Correct dataset is identically transformed")
    public void testTransformCorrectData(){
        String rowInJson1 = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String rowInJson2 = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892");
        String rowInJson3 = createJsonInString("3", "Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com","1234567893");
        String[] tableData = {rowInJson1, rowInJson2, rowInJson3};
        String queryName = "tTestQuery1";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891", rowInJson1, 1));
        expectedResult.add(RowFactory.create("2","Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892", rowInJson2, 2));
        expectedResult.add(RowFactory.create("3","Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com","1234567893", rowInJson3, 3));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Dataset size is the same after transformation")
    public void testTransformCorrectDataSize(){
        String rowInJson1 = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String rowInJson2 = createJsonInString("2", "Title 2","http://www.url2.com",
                "Publisher 2","t","abcdef2","www.publisher2.com","1234567892");
        String rowInJson3 = createJsonInString("3", "Title 3","http://www.url3.com",
                "Publisher 3","b","abcdef3","www.publisher3.com","1234567893");
        String[] tableData = {rowInJson1, rowInJson2, rowInJson3};
        String queryName = "tTestQuery2";
        List<Row> result = makeTransformation(tableData, queryName);
        int expectedRows = 3;
        int actualRows = result.size();
        assertEquals(expectedRows, actualRows);
    }

    @Test
    @DisplayName("Row with all null values is identically transformed")
    public void testTransformAllNull(){
        String rowInJson = createJsonInString(null, null,null,
                null,null,null,null,null);
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery3";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("null","null","null",
                "null","null","null","null","null", rowInJson, null));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with some null values is identically transformed")
    public void testTransformSomeNull(){
        String rowInJson = createJsonInString("1", null,"http://www.url1.com",
                "Publisher 1",null,"abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery4";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","null","http://www.url1.com",
                "Publisher 1","null","abcdef1","www.publisher1.com","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }


    @Test
    @DisplayName("Row with null ID is identically transformed")
    public void testTransformNullID(){
        String rowInJson = createJsonInString(null, "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery5";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("null","Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891", rowInJson, null));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null TITLE is identically transformed")
    public void testTransformNullTITLE(){
        String rowInJson = createJsonInString("1", null,"http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery6";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","null","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null URL is identically transformed")
    public void testTransformNullURL(){
        String rowInJson = createJsonInString("1", "Title 1",null,
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery7";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","null",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null PUBLISHER is identically transformed")
    public void testTransformNullPUBLISHER(){
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                null,"e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery8";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","http://www.url1.com",
                "null","e","abcdef1","www.publisher1.com","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null CATEGORY is identically transformed")
    public void testTransformNullCATEGOTY(){
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1",null,"abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery9";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","http://www.url1.com",
                "Publisher 1","null","abcdef1","www.publisher1.com","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null STORY is identically transformed")
    public void testTransformNullSTORY(){
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e",null,"www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery10";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","http://www.url1.com",
                "Publisher 1","e","null","www.publisher1.com","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with null HOSTNAME is identically transformed")
    public void testTransformNullHOSTNAME(){
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1",null,"1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery11";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","null","1234567891", rowInJson, 1));
        assertEquals(expectedResult, result);
    }


    @Test
    @DisplayName("Row with null TIMESTAMP is identically transformed")
    public void testTransformNullTIMESTAMP(){
        String rowInJson = createJsonInString("1", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com",null);
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery12";
        List<Row> result = makeTransformation(tableData, queryName);
        List<Row> expectedResult = new ArrayList<>();
        expectedResult.add(RowFactory.create("1","Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","null", rowInJson, 1));
        assertEquals(expectedResult, result);
    }

    @Test
    @DisplayName("Row with string ID is transformed to row with null in ID column")
    public void testStringID(){
        String rowInJson = createJsonInString("someString", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery13";
        List<Row> resultList = makeTransformation(tableData, queryName);
        Row resultRow = resultList.get(0);
        assertEquals(null, resultRow.get(9));
    }

    @Test
    @DisplayName("Row with integer ID is transformed to row with long in ID column")
    public void testIntID(){
        String rowInJson = createJsonInString("101", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery14";
        List<Row> resultList = makeTransformation(tableData, queryName);
        Row resultRow = resultList.get(0);
        long expectedResult = 101;
        assertEquals(expectedResult, resultRow.get(9));
    }

    @Test
    @DisplayName("Row with real ID number is transformed to row with null in ID column")
    public void testRealIDNumber(){
        String rowInJson = createJsonInString("2.5", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery15";
        List<Row> resultList = makeTransformation(tableData, queryName);
        Row resultRow = resultList.get(0);
        assertEquals(null, resultRow.get(9));
    }

    @Test
    @DisplayName("Row with real ID number is transformed to row with null in ID column")
    public void testNegativeIDNumber(){
        String rowInJson = createJsonInString("-101", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery16";
        List<Row> resultList = makeTransformation(tableData, queryName);
        Row resultRow = resultList.get(0);
        assertEquals(null, resultRow.get(9));
    }

    @Test
    @DisplayName("Row with zero ID is transformed to row with zero in ID column")
    public void testZeroID(){
        String rowInJson = createJsonInString("0", "Title 1","http://www.url1.com",
                "Publisher 1","e","abcdef1","www.publisher1.com","1234567891");
        String[] tableData = {rowInJson};
        String queryName = "tTestQuery17";
        List<Row> resultList = makeTransformation(tableData, queryName);
        Row resultRow = resultList.get(0);
        long expectedResult = 0;
        assertEquals(expectedResult, resultRow.get(9));
    }

    private List<Row> makeTransformation(String[] tableData, String queryName) {
        StreamingPipeline rowInserter = new StreamingPipeline();
        SparkSession spark = rowInserter.getSparkSession();
        Option<Object> numPartitions = Option.apply(1);
        MemoryStream<String> testStream = new MemoryStream<>(1, spark.sqlContext(), numPartitions, Encoders.STRING());
        testStream.addData(convertListToSeq(Arrays.asList(tableData)));
        Dataset<String> sessions = testStream.toDS();
        Dataset filteredDF = rowInserter.transformStreamingDataset(sessions.toDF());

        StreamingQuery streamingQuery = null;
        try {
            streamingQuery = filteredDF
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .outputMode("append")
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        streamingQuery.processAllAvailable();

        List<Row> result = spark.sql("select * from " + queryName).collectAsList();
        return result;
    }
    private Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
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
