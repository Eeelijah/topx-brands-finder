package ru.lamoda.bigdata.topxbrandsfinder.config;

import com.typesafe.config.Config;
import lombok.Value;

@Value
public class AppConfig {

    private String dictDir;
    private String logsDir;
    private String destDir;
    private int topX;
    private SparkParams sparkParams;

    public AppConfig(Config config) {
        sparkParams = new SparkParams(config);
        dictDir = config.getString("app.dictDir");
        logsDir = config.getString("app.logsDir");
        destDir = config.getString("app.destDir");
        topX = config.getInt("app.topX");
    }
}
