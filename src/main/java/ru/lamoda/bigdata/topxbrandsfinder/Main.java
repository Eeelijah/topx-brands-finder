package ru.lamoda.bigdata.topxbrandsfinder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.lamoda.bigdata.topxbrandsfinder.config.SampleParams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) throws IOException {

        Path overloadConfigPath = Paths.get("./app.conf");
        Config config;
        if (Files.isReadable(overloadConfigPath)) {
            config = ConfigFactory.parseFile(new File(overloadConfigPath.toUri()));
        } else {
            config = ConfigFactory.parseResources("app.conf");
        }

        if (config.getString("app.mode").equals("test")) {
            File samplesDirectory = new File(config.getString("test.samplesDir"));
            if (!samplesDirectory.exists()) {
                new SampleGenerator(new SampleParams(config)).createTestData();
            }
            new Application(config).run();
        } else if (config.getString("app.mode").equals("prod")) {
            new Application(config).run();
        } else {
            throw new RuntimeException("There is not such mode. Mode should be specified: prod or test");
        }
    }
}
