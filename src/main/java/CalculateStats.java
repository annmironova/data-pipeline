import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

import java.util.concurrent.TimeoutException;

public class CalculateStats {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        String brokers = "localhost:9092";
        String topicName = "uci_news";

        SparkSession spark = SparkSession
                .builder()
                .appName("Csv to Kafka Json messages")
                .config("spark.master", "local[2]")
                .getOrCreate();

        Dataset<Row> dataset = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topicName)
                .load();


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

        Dataset<Row> datasetTransformed = dataset
                .selectExpr("CAST(value as string)")
                .select(from_json(col("value"), schema));

        Dataset<Row> datasetAggregated = datasetTransformed.select("from_json(value).PUBLISHER", "from_json(value).TIMESTAMP")
                .groupBy("PUBLISHER").count();

        StreamingQuery streamingQuery = datasetAggregated.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        streamingQuery.awaitTermination();

    }
}