package com.reiser.juc.thread_pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: reiserx
 * Date:2022/1/18
 * Des:固定大小的线程池
 */
public class NewFixedThreadExecutorDemo {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(16);

        for (int i = 0; i < 100; i++) {
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
