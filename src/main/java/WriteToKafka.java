import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;


public class WriteToKafka {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("C:/Users/saint/IdeaProjects/data-pipeline/src/main/resources/uci-news-aggregator.csv");
        df.show();

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> df1 = df.map((MapFunction<Row, String>) row -> row.toString(),stringEncoder);
        df1.printSchema();
        df1.write().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "dataset")
                .save();

    }


}
