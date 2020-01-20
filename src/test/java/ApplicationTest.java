import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;

public class ApplicationTest {

    @Test
    public void getTopXByCount() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Top X Brands Finder")
                .getOrCreate();

        Dataset<Row> logs = sparkSession
                .read()
                .option("header", "true") //first line in file has headers
                .csv("src/test/resources/logs/0000.csv");
        Dataset<Row> dictionary = sparkSession
                .read()
                .option("header", "true") //first line in file has headers
                .csv("src/test/resources/dictionary/0000.csv");

        String user = "3bf763a9-c169-4df4-9ad7-0a";
        Dataset<Row> userProducts = logs
                .where(logs.col("session_id").equalTo(user))
                .groupBy(logs.col("url"))
                .count();
        ;
        Row row = userProducts.orderBy(desc("count")).first();
        String[] urlParts = row.getString(0).split("/");
        String itemId = urlParts[urlParts.length - 1];
        String brandName = dictionary.where(dictionary.col("item_id").equalTo(itemId)).first().getString(0);
        Assert.assertThat(brandName, Is.is("Bershka"));
    }

    @Test
    public void getTopXByTime() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Top X Brands Finder")
                .getOrCreate();

        Dataset<Row> logs = sparkSession
                .read()
                .option("header", "true") //first line in file has headers
                .csv("src/test/resources/logs/0000.csv");
        Dataset<Row> dictionary = sparkSession
                .read()
                .option("header", "true") //first line in file has headers
                .csv("src/test/resources/dictionary/0000.csv");

        String user = "3bf763a9-c169-4df4-9ad7-0a";
        Object take = logs.join(
                dictionary,
                dictionary.col("item_id").equalTo(logs.col("url").substr(19, 100)),
                "left"
        )
                .sort(desc("timestamp"))
                .withColumn("prev_timestamp",
                        coalesce(lag(col("timestamp"), 1).over(Window.partitionBy(col("session_id")).orderBy(col("timestamp").desc())), unix_timestamp()))
                .withColumn("diff", col("prev_timestamp").minus(col("timestamp")))
                .groupBy(col("session_id"), col("brand_name"))
                .sum("diff").withColumnRenamed("sum(diff)", "time")
                .withColumn("rank", row_number().over(Window.partitionBy("session_id").orderBy(desc("time"))))
                .filter("rank <= 1").take(1);
        ;

        String brandName = ((Row[]) take)[0].getString(1);
        Assert.assertThat(brandName, Is.is("RalphLauren"));
    }
}
