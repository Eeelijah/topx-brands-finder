package ru.lamoda.bigdata.topxbrandsfinder;

import lombok.Getter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import ru.lamoda.bigdata.topxbrandsfinder.config.AppConfig;
import ru.lamoda.bigdata.topxbrandsfinder.config.SparkParams;

@Getter
class SparkContextHolder {

    private final SparkSession sparkSession;
    private final JavaSparkContext javaSparkContext;

    SparkContextHolder(AppConfig appConfig) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());

        SparkSession.Builder sessionConfigBuilder;

        SparkParams sparkParams = appConfig.getSparkParams();
        if (sparkParams.getMaster().equals("local")) {
            sparkConf.setMaster("local");
            sessionConfigBuilder = SparkSession.builder().config(sparkConf);
        } else {
            sessionConfigBuilder = SparkSession.builder().config(sparkConf);
            sessionConfigBuilder
                    .master(sparkParams.getMaster())
                    .config("hive.metastore.sasl.enabled", "true")
                    .config("hive.security.authorization.enabled", "false")
                    .config("hive.metastore.execute.setugi", "true")
                    .config("spark.yarn.scheduler.heartbeat.interval-ms", 10000000)
                    .config("spark.network.timeout", 10000000)
                    .config("spark.executor.heartbeatInterval ", 10000000)
                    .config("spark.yarn.queue", sparkParams.getQueue())
                    .config("spark.driver.memory", sparkParams.getDriverMemory())
                    .config("spark.executor.memory", sparkParams.getExecutorMemory())
                    .config("spark.shuffle.service.enabled", "true")
                    .config("spark.dynamicAllocation.enabled", "true")
                    .config("spark.dynamicAllocation.maxExecutors", sparkParams.getMaxExecutors())
                    .config("spark.sql.shuffle.partitions", "1000")
                    .config("spark.sql.orc.filterPushdown", "true")
                    .config("mapreduce.fileoutputcommitter.algorithm.version", "2");
        }

        sparkSession = sessionConfigBuilder.appName("Top 5 brand finder").getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }
}
