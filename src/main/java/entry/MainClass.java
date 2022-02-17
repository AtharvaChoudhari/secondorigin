package entry;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */

public class MainClass {
    public static final String HEADERS = "header";
    static Logger logger = Logger.getLogger("MyLog");
    private static final String CUSTOMER_ID = "customer_id";
    private static final String ORDER_CUSTOMER_ID = "order_customer_id";
    private static final String ORDER_ITEM_SUBTOTAL = "order_item_subtotal";
    private static final String PRODUCT_ID = "product_id";
    private static final String CATEGORY_ID = "category_id";
    private static final String CUSTOMER_FIRST_NAME = "customer_first_name";
    private static final String CUSTOMER_LAST_NAME = "customer_last_name";
    private static final String CUSTOMER_REVENUE = "customer_revenue";

    public static SparkSession getSparkSession(){
        return SparkSession.builder().appName("uses cases").master("local").getOrCreate();
    }


    public static void main(String[] args )  {
        String customer = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\customers\\part*";
        String category = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\categories\\part*";
        String department = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\departments\\part*";
        String orderitems = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\order_items\\part*";
        String orders = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\orders\\part*";
        String product = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\products\\part*";


        Dataset<Row> dCustomer = ReadData.getDataset(getSparkSession(),customer);
        Dataset<Row> dfCategory = ReadData.getDataset(getSparkSession(),category);
        Dataset<Row> dfDepartment = ReadData.getDataset(getSparkSession(),department);
        Dataset<Row> dfOrderItems = ReadData.getDataset(getSparkSession(),orderitems);
        Dataset<Row> dfOrder = ReadData.getDataset(getSparkSession(),orders);
        Dataset<Row> dfProduct =  ReadData.getDataset(getSparkSession(),product);


        Dataset <Row> customerOrderCount = dfOrder.filter("order_date LIKE '2014-01%'")
                .join(dCustomer, dCustomer.col(CUSTOMER_ID)
                        .equalTo(dfOrder.col(ORDER_CUSTOMER_ID))).select(
                        dCustomer.col("customer_fname").alias(CUSTOMER_FIRST_NAME),
                        dCustomer.col("customer_lname").alias(CUSTOMER_LAST_NAME),
                        dfOrder.col("order_status"),
                        dCustomer.col(CUSTOMER_ID))
                .groupBy(CUSTOMER_ID,CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME)
                .agg(count("order_status").alias("customers_order_count"))
                .orderBy(col("customers_order_count").desc(), (col(CUSTOMER_ID).asc()))
                ;
        customerOrderCount.show();


        Dataset <Row> dormantCustomers = dfOrder
                .filter("order_date LIKE '2014-01%'AND order_status IS NULL")
                .join(dCustomer, dCustomer.col(CUSTOMER_ID)
                        .equalTo(dfOrder.col(ORDER_CUSTOMER_ID)))
                .select(dCustomer.col("*"));

        dormantCustomers.show();


        Dataset <Row> revenuePerCustomer = dfOrder
                .join(dCustomer,
                        dCustomer.col(CUSTOMER_ID)
                                .equalTo(dfOrder.col(ORDER_CUSTOMER_ID)))
                .join(
                        dfOrderItems,dfOrder.col("order_id").
                                equalTo(dfOrderItems.col("order_item_id"))).select(
                        dCustomer.col(CUSTOMER_ID),
                        dCustomer.col("customer_fname").alias(CUSTOMER_FIRST_NAME),
                        dCustomer.col("customer_lname").alias(CUSTOMER_LAST_NAME),
                        dfOrderItems.col(ORDER_ITEM_SUBTOTAL)
                )
                .groupBy(CUSTOMER_ID,CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME)
                .agg(round(sum(dfOrderItems.col(ORDER_ITEM_SUBTOTAL)),2)
                        .alias(CUSTOMER_REVENUE)
                ).orderBy(col(CUSTOMER_REVENUE).desc());

        revenuePerCustomer.show();


        Dataset <Row> revenuePerCategory = dfOrder
                .join(
                        dfOrderItems,dfOrder.col("order_id").
                                equalTo(dfOrderItems.col("order_item_id")))
                .join(
                        dfProduct,dfProduct.col(PRODUCT_ID)
                                .equalTo(dfOrderItems.col("order_item_product_id")))
                .join(
                        dfCategory,dfCategory.col(CATEGORY_ID)
                                .equalTo(dfProduct.col("product_category_id")))
                .select(dfCategory.col("*"),dfOrderItems.col(ORDER_ITEM_SUBTOTAL))
                .groupBy(CATEGORY_ID,"category_department_id","category_name")

                .agg(round(sum(dfOrderItems.col(ORDER_ITEM_SUBTOTAL))).alias(CUSTOMER_REVENUE))
                .orderBy(CATEGORY_ID)
                ;

        revenuePerCategory.show();


        Dataset<Row> finalrev1 =dfDepartment.
                join(
                        dfCategory,dfCategory.col("category_department_id")
                                .equalTo(dfDepartment.col("department_id")))
                .join(
                        dfProduct , dfProduct.col("product_category_id")
                                .equalTo(dfCategory.col(CATEGORY_ID)))
                .select(dfDepartment.col("*"),
                        dfProduct.col(PRODUCT_ID))
                .groupBy("department_id","department_name")
                .agg(count(dfProduct.col(PRODUCT_ID)).alias("product_count"));
        finalrev1.show();

        WriteData writeData = new WriteData();


        writeData.writeData(customerOrderCount,"C:\\Users\\Atharva Choudhari\\ProjUse\\CustomerOrderCount");
        writeData.writeData(dormantCustomers,"C:\\Users\\Atharva Choudhari\\ProjUse\\DormantCustomers");
        writeData.writeData(revenuePerCustomer,"C:\\Users\\Atharva Choudhari\\ProjUse\\RevenuePerUser");
        writeData.writeData(revenuePerCategory,"C:\\Users\\Atharva Choudhari\\ProjUse\\RevenuePerCategory");
        writeData.writeData(finalrev1,"C:\\Users\\Atharva Choudhari\\ProjUse\\finalout1");



    }
}




