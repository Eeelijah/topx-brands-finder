package ru.lamoda.bigdata.topxbrandsfinder.config;

import com.typesafe.config.Config;
import lombok.Value;

@Value
public class SampleParams {

    public String CSV_SAMPLE_NAME;
    public String SAMPLES_DIR;
    public String BRAND_NAMES;
    public String LOGS_DIR;
    public String DICTIONARY_DIR;
    public String LOGS_HEADER;
    public String BRAND_HEADER;
    public String BASE_URL;
    public int DAYS_TO_GENERATE;
    public int NUMBER_OF_USERS;

    public SampleParams(Config config) {
        CSV_SAMPLE_NAME = config.getString("test.csvSampleName");
        SAMPLES_DIR = config.getString("test.samplesDir");
        BRAND_NAMES = config.getString("test.brandNames");
        LOGS_DIR = config.getString("test.logsDir");
        DICTIONARY_DIR = config.getString("test.dictionaryDir");
        LOGS_HEADER = config.getString("test.logsHeader");
        BRAND_HEADER = config.getString("test.dictHeader");
        BASE_URL = config.getString("test.baseUrl");
        DAYS_TO_GENERATE = config.getInt("test.daysToGenerate");
        NUMBER_OF_USERS = config.getInt("test.numberOfUsers");
    }
}
