package com.reiser.juc.thread_pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: reiserx
 * Date:2022/1/18
 * Des:会不断的创建线、直到崩溃
 */
public class NewCachedThreadExecutorDemo {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i = 0; i < 10000; i++) {
            int no = i;

            executorService.execute(() -> {
                System.out.println(Thread.currentThread().getName()+" start: " + no);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName()+" end: " + no);
            });
        }

        executorService.shutdown();
        System.out.println("Main Thread End!");
    }
}
