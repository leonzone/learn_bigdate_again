package com.reiser.juc.thread_practice.wc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author: reiserx
 * Date:2021/2/14
 * Des:多线程编程练习，多线程版 WordCount
 */
public class WordCount {
    private int threadNum;
    private ExecutorService threadPool;

    public static void main(String[] args) throws FileNotFoundException, ExecutionException, InterruptedException {

        WordCount wc = new WordCount(5);
        System.out.println(wc.count(new File("/Users/reiserx/code/java/learn_java/src/main/resources/test.txt")));

    }

    public WordCount(int threadNum) {
        this.threadNum = threadNum;
        //Step1. 启动，使用 Executors 创建线程池
        this.threadPool = Executors.newFixedThreadPool(threadNum);
    }


    public Map<String, Integer> count(File file) throws FileNotFoundException, ExecutionException, InterruptedException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        List<Future<Map<String, Integer>>> futureList = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            Future<Map<String, Integer>> mapFuture = threadPool.submit(new Callable<Map<String, Integer>>() {
                @Override
                public Map<String, Integer> call() throws Exception {
                    Map<String, Integer> resultMap = new HashMap<>();
                    String line = null;
                    // Step2. 分割，bufferedReader readLine读取每一行，并每个线程通过 Callable 返回统计结果
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            resultMap.put(word, resultMap.getOrDefault(word, 0) + 1);
                        }
                    }
                    // Step3.计算，使用 split 分割单词
                    return resultMap;
                }
            });
            futureList.add(mapFuture);
        }

        // Step4.汇总，Future get 获取到每个线程的结果
        Map<String, Integer> finalMap = new HashMap<>();
        for (Future<Map<String, Integer>> mapFuture : futureList) {
            Map<String, Integer> map = mapFuture.get();
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {
                finalMap.put(entry.getKey(), entry.getValue() + finalMap.getOrDefault(entry.getKey(), 0));
            }

        }
        threadPool.shutdown();
        return finalMap;
    }

}