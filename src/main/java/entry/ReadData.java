package entry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadData {
    public static Dataset<Row> getDataset(SparkSession session, String path){
        Dataset<Row> dataset = session.read().option("header", "true").csv(path);
        return dataset;
    }
}