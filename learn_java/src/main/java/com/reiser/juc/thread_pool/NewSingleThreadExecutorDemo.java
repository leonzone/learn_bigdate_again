package com.reiser.juc.thread_pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: reiserx
 * Date:2022/1/18
 * Des:单线程的线程池
 */
public class NewSingleThreadExecutorDemo {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        for (int i = 0; i < 10; i++) {
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
