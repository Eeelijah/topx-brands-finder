package ru.lamoda.bigdata.topxbrandsfinder;

import ru.lamoda.bigdata.topxbrandsfinder.config.SampleParams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SampleGenerator {

    private static SampleParams sampleParams;

    public SampleGenerator(SampleParams sampleParams) {
        SampleGenerator.sampleParams = sampleParams;
    }

    public void createTestData() throws IOException {
        Random random = new Random();

        LocalTime midnight = LocalTime.MIDNIGHT;
        LocalDate today = LocalDate.now().minusDays(1 + sampleParams.DAYS_TO_GENERATE);
        LocalDateTime dateFrom = LocalDateTime.of(today, midnight);

        if (!new File(sampleParams.DICTIONARY_DIR).mkdirs() || !new File(sampleParams.LOGS_DIR).mkdirs()) {
            throw new IOException("Could not create samples directory");
        }

        Map<String, String> itemToBrandMap = generateProducts();
        ArrayList<String> items = new ArrayList<>(itemToBrandMap.keySet());
        List<String> productsCsv = itemToBrandMap
                .entrySet()
                .stream()
                .map(entry -> String.format("%s,%s", entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
        productsCsv.add(0, sampleParams.BRAND_HEADER);
        try {
            Files.write(Paths.get(sampleParams.DICTIONARY_DIR + sampleParams.CSV_SAMPLE_NAME), productsCsv);
        } catch (IOException e) {
            System.out.println("Could not create csv file");
        }

        List<String> userIds = Stream
                .generate(UUID::randomUUID)
                .limit(sampleParams.NUMBER_OF_USERS)
                .map(UUID::toString)
                .map(s -> s.substring(0, 26))
                .collect(Collectors.toList());

        DateTimeFormatter partitionFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        for (int i = 0; i <= sampleParams.DAYS_TO_GENERATE; i++) {
            List<String> dayLog = new LinkedList<>();
            dayLog.add(sampleParams.LOGS_HEADER);
            LocalDateTime currentDayTime = dateFrom.plusDays(i);
            LocalDateTime endDayTime = currentDayTime.plusDays(1);
            String partition = partitionFormat.format(currentDayTime);
            while (currentDayTime.isBefore(endDayTime)) {
                currentDayTime = currentDayTime
                        .plusSeconds(10 + random.nextInt(60))
                        .plusNanos(random.nextInt(1000000000));
                String user = userIds.get(random.nextInt(userIds.size()));
                String item = sampleParams.BASE_URL + items.get(random.nextInt(items.size()));
                long timestamp = Timestamp.valueOf(currentDayTime).getTime();
                String line = String.format("%s,%s,%s,%s", user, item, timestamp, partition);
                dayLog.add(line);
            }
            dayLog.remove(dayLog.size() - 1);
            String dirPath = sampleParams.LOGS_DIR + partition;
            File directory = new File(dirPath);
            directory.mkdir();
            try {
                Files.write(Paths.get(dirPath + "/" + sampleParams.CSV_SAMPLE_NAME), dayLog);
            } catch (IOException e) {
                System.out.println("Could not create csv file " + e.toString());
            }
        }

    }

    private Map<String, String> generateProducts() {
        Random random = new Random();
        int minProducts = 2;
        int maxProducts = 5;
        List<String> brands = new ArrayList<>();

        try (Stream<String> lines = Files.lines(Paths.get(sampleParams.BRAND_NAMES))) {
            brands = lines.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("Could not read file");
        }

        HashMap<String, String> productAndBrand = new HashMap<>();
        for (String brand : brands) {
            int numberOfItems = random.nextInt((maxProducts - minProducts) + 1) + minProducts;
            for (int currentItem = 1; currentItem <= numberOfItems; currentItem++) {
                String productName = brand.substring(0, 4) + currentItem;
                productAndBrand.put(productName, brand);
            }
        }

        return productAndBrand;
    }
}
