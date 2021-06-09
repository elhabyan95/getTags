package com.mycompany.sparktest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TagWordCounter {

    private static final String COMMA_DELIMITER = ",";

    public static void getTagWordCount() throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("tagCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        JavaRDD<String> videos = sparkContext.textFile("USvideos.csv");

        JavaRDD<String> tags = videos
                .map(TagWordCounter::extractTag)
                .filter(StringUtils::isNotBlank);
        
        
        JavaRDD<String> words = tags.flatMap(tag -> Arrays.asList(tag
                .toLowerCase()
                .trim()
                
                .split("//|")).iterator());
        System.out.println(words.toString());
        
        
        Map<String, Long> wordCounts = words.countByValue();
        
        
        List<Map.Entry> sorted = wordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    private static String extractTag(String videoLine) {
        try {
            String str = videoLine.split(COMMA_DELIMITER)[6];
            return str;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }

    }
}


