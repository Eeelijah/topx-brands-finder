package ru.lamoda.bigdata.topxbrandsfinder;

import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.lamoda.bigdata.topxbrandsfinder.config.AppConfig;

import java.io.IOException;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class.getName());

    private final SparkContextHolder sparkContextHolder;
    private final AppConfig appConfig;

    Application(Config config) {
        appConfig = new AppConfig(config);
        sparkContextHolder = new SparkContextHolder(appConfig);
    }

    public void run() throws IOException {
        writeToCsv(getTopXByCount(), "byCount");
        writeToCsv(getTopXByTime(), "byTime");
    }

    private Dataset<Row> getTopXByCount() {
        Dataset<Row> logs = sparkContextHolder.getSparkSession()
                .read()
                .option("header", "true") //first line in file has headers
                .csv(appConfig.getLogsDir() + "*");
        Dataset<Row> dictionary = sparkContextHolder.getSparkSession()
                .read()
                .option("header", "true") //first line in file has headers
                .csv(appConfig.getDictDir() + "*");
        return logs.join(
                broadcast(dictionary),
                dictionary.col("item_id").equalTo(logs.col("url").substr(19, 100)),
                "left"
        )
                .groupBy("session_id", "brand_name")
                .count()
                .withColumn("rank", row_number().over(Window.partitionBy("session_id").orderBy(desc("count"))))
                .filter("rank <= " + appConfig.getTopX());
    }

    private Dataset<Row> getTopXByTime() {
        Dataset<Row> logs = sparkContextHolder.getSparkSession()
                .read()
                .option("header", "true") //first line in file has headers
                .csv(appConfig.getLogsDir() + "*");
        Dataset<Row> dictionary = sparkContextHolder.getSparkSession()
                .read()
                .option("header", "true") //first line in file has headers
                .csv(appConfig.getDictDir() + "*");
        return logs.join(
                broadcast(dictionary),
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
                .filter("rank <= " + appConfig.getTopX());
    }

    private void writeToCsv(Dataset<Row> dataset, String type) {
        dataset.repartition(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(appConfig.getDestDir() + type + "/" + LocalDate.now().toString());
    }
}
