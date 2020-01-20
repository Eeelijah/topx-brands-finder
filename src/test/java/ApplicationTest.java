import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import ru.lamoda.bigdata.topxbrandsfinder.Application;

import java.io.File;
import java.nio.file.Paths;

public class ApplicationTest {

    @Test
    public void getTopXByCount() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Top X Brands Finder")
                .getOrCreate();

        Application app = new Application(ConfigFactory.parseFile(new File(Paths.get("src/main/resources/app.conf").toUri())));
        Dataset<Row> topXByCount = app.getTopXByCount("src/test/resources/logs/0000.csv", "src/test/resources/dictionary/0000.csv");
        String brandName = ((Row[]) topXByCount.take(1))[0].getString(1);
        Assert.assertThat(brandName, Is.is("Bershka"));
    }

    @Test
    public void getTopXByTime() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Top X Brands Finder")
                .getOrCreate();
        Application app = new Application(ConfigFactory.parseFile(new File(Paths.get("src/main/resources/app.conf").toUri())));
        Dataset<Row> topXByCount = app.getTopXByTime("src/test/resources/logs/0000.csv", "src/test/resources/dictionary/0000.csv");
        String brandName = ((Row[]) topXByCount.take(1))[0].getString(1);
        Assert.assertThat(brandName, Is.is("RalphLauren"));
    }
}
