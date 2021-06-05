/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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

/**
 *
 * @author elhabyan
 */
public class TitleWordCounter {

    private static final String COMMA_DELIMITER = ",";

    public static void getTitleWordCount() throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> videos = sparkContext.textFile("F:\\USvideos.csv");

        JavaRDD<String> titles = videos
                .map(TitleWordCounter::extractTitle)
                .filter(StringUtils::isNotBlank);
        
        
        JavaRDD<String> words = titles.flatMap(title -> Arrays.asList(title
                .toLowerCase()
                .trim()
                .replaceAll("\\p{Punct}", " ")
                .split(" ")).iterator());
        System.out.println(words.toString());

        Map<String, Long> wordCounts = words.countByValue();
        
        
        List<Map.Entry> sorted = wordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());

        sorted.forEach(entry -> {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        });

    }

    public static String extractTitle(String videoLine) {
        try {
            return videoLine.split(COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }

    }
}
