package entry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriteData {

    public void writeData(Dataset<Row> writeLocation, String path){
        writeLocation.coalesce(1).write().option("header","true").mode("overwrite").csv(path);

    }
}