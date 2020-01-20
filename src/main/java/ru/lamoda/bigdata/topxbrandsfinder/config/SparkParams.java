package ru.lamoda.bigdata.topxbrandsfinder.config;

import com.typesafe.config.Config;
import lombok.Value;

@Value
public class SparkParams {

    private String master;
    private String queue;
    private String driverMemory;
    private String executorMemory;
    private String executorCores;
    private String maxExecutors;
    private String thriftUrl;

    SparkParams(Config conf) {
        master = conf.getString("spark.master");
        queue = conf.getString("spark.queue");
        driverMemory = conf.getString("spark.driverMemory");
        executorMemory = conf.getString("spark.executorMemory");
        executorCores = conf.getString("spark.executorCores");
        maxExecutors = conf.getString("spark.maxExecutors");
        thriftUrl = conf.getString("spark.thriftUrl");
    }
}
