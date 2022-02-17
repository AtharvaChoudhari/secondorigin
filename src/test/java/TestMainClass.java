import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import static org.junit.Assert.assertEquals;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.sum;
import java.util.logging.Logger;


public class TestMainClass {

    public static final String HEADERS = "header";
    static Logger logger = Logger.getLogger("MyLog");
    public static final SparkSession sprkSecc = SparkSession.builder().appName("uses cases").master("local").getOrCreate();
    public static final String F_STRING = "Test case";
    public static final String L_STRING = "is success";
    private Dataset<Row> customerOrderCountread;
    private Dataset<Row> customerOrderCount;
    private Dataset<Row> dormantCustomersread;
    private Dataset<Row> dormantCustomers;
    private Dataset<Row> revenuePerCategoryread;
    private Dataset<Row> revenuePerCategory;
    private Dataset<Row> revenuePerCustomerread;
    private Dataset<Row> revenuePerCustomer;
    private  Dataset<Row> finalrev1read;
    private Dataset<Row> finalrev1;
    public static void main(String[] args) throws AnalysisException {

        String customer = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\customers\\part*";
        String category = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\categories\\part*";
        String department = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\departments\\part*";
        String orderitems = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\order_items\\part*";
        String orders = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\orders\\part*";
        String product = "C:\\Users\\Atharva Choudhari\\UseCase\\retail_db\\products\\part*";

        Dataset<Row> dCustomer = sprkSecc.read().option(HEADERS, "true").csv(customer);
        Dataset<Row> dfCategory = sprkSecc.read().option(HEADERS, "true").csv(category);
        Dataset<Row> dfDepartment = sprkSecc.read().option(HEADERS, "true").csv(department);
        Dataset<Row> dfOrderItems = sprkSecc.read().option(HEADERS, "true").csv(orderitems);
        Dataset<Row> dfOrder = sprkSecc.read().option(HEADERS, "true").csv(orders);
        Dataset<Row> dfProduct = sprkSecc.read().option(HEADERS, "true").csv(product);

        final String CUSTOMER_ID = "customer_id";
        final String ORDER_CUSTOMER_ID = "order_customer_id";
        final String ORDER_ITEM_SUBTOTAL = "order_item_subtotal";
        final String PRODUCT_ID = "product_id";
        final String CATEGORY_ID = "category_id";
        final String CUSTOMER_FIRST_NAME = "customer_first_name";
        final String CUSTOMER_LAST_NAME = "customer_last_name";
        final String CUSTOMER_REVENUE = "customer_revenue";

        Dataset<Row> customerOrderCountread =sprkSecc.read().option("header", "true").csv("C:\\Users\\Atharva Choudhari\\ProjUse\\CustomerOrderCount");
        Dataset <Row> customerOrderCount = dfOrder.filter("order_date LIKE '2014-01%'")
                .join(dCustomer, dCustomer.col(CUSTOMER_ID)
                        .equalTo(dfOrder.col(ORDER_CUSTOMER_ID))).select(
                        dCustomer.col("customer_fname").alias(CUSTOMER_FIRST_NAME),
                        dCustomer.col("customer_lname").alias(CUSTOMER_LAST_NAME),
                        dfOrder.col("order_status"),
                        dCustomer.col(CUSTOMER_ID))
                .groupBy(CUSTOMER_ID,CUSTOMER_FIRST_NAME,CUSTOMER_LAST_NAME)
                .agg(count("order_status").alias("customers_order_count"))
                .orderBy(col(CUSTOMER_ID).asc(),col("customers_order_count").desc());

        Dataset<Row> dormantCustomersread = sprkSecc.read().option("header", "true").csv("C:\\Users\\Atharva Choudhari\\ProjUse\\DormantCustomers");
        Dataset <Row> dormantCustomers = dfOrder
                .filter("order_date LIKE '2014-01%'AND order_status IS NULL")
                .join(dCustomer, dCustomer.col(CUSTOMER_ID)
                        .equalTo(dfOrder.col(ORDER_CUSTOMER_ID)))
                .select(dCustomer.col("*"));

        Dataset<Row> revenuePerCategoryread = sprkSecc.read().option("header", "true").csv("C:\\Users\\Atharva Choudhari\\ProjUse\\RevenuePerCategory");
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

        Dataset<Row> revenuePerCustomerread = sprkSecc.read().option("header", "true").csv("C:\\Users\\Atharva Choudhari\\ProjUse\\RevenuePerUser");
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

        Dataset<Row> finalrev1read = sprkSecc.read().option("header", "true").csv("C:\\Users\\Atharva Choudhari\\ProjUse\\finalout1");
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

    }
    @Test
    public void testUseCaseOne(){
        System.out.println(customerOrderCountread);
        System.out.println(customerOrderCount);
        assertEquals("This method is for testing customer order count",customerOrderCountread,customerOrderCount);


    }

    @Test
    public void testUseCaseTwo(){
        System.out.println(dormantCustomersread);
        System.out.println(dormantCustomers);
        assertEquals("This method is for testing dormant customer",dormantCustomersread,dormantCustomers);


    }

    @Test
    public void testUseCaseThree(){
        System.out.println(revenuePerCategoryread);
        System.out.println(revenuePerCategory);
        assertEquals("This method is for testing revenue per customer",revenuePerCategoryread,revenuePerCategory);


    }

    @Test
    public void testUseCaseFour(){
        System.out.println(revenuePerCustomerread);
        System.out.println(revenuePerCustomer);
        assertEquals("This method is for testing revenue per category",revenuePerCustomerread,revenuePerCustomer);


    }

    @Test
    public void testUseCaseFive(){
        System.out.println(finalrev1read);
        System.out.println(finalrev1);
        assertEquals("This method is for testing product count per department",finalrev1read,finalrev1);

        logger.info(F_STRING+"five"+L_STRING);
    }

}

