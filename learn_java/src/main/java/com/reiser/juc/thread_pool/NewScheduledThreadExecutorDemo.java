package com.reiser.juc.thread_pool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author: reiserx
 * Date:2022/1/18
 * Des:固定大小的线程池,并且一批一批的执行
 */
public class NewScheduledThreadExecutorDemo {
    public static void main(String[] args) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(16);

        for (int i = 0; i < 100; i++) {
            int no = i;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + " start: " + no);
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + " end: " + no);
                }
            };

            executorService.schedule(runnable, 10, TimeUnit.SECONDS);
        }

        executorService.shutdown();
        System.out.println("Main Thread End!");
    }
}
