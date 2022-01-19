package com.reiser.juc.thread_pool;

import java.util.concurrent.*;

/**
 * @author: reiserx
 * Date:2022/1/18
 * Des:自定义线程池
 */
public class ExecutorServiceDemo {

    public static void main(String[] args) {
        // 核心线程数
        int coreSize = Runtime.getRuntime().availableProcessors();
        //最大线程数
        int maxSize = Runtime.getRuntime().availableProcessors() * 2;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingDeque<>(500);
        ExecutorService executorService = new ThreadPoolExecutor(coreSize, maxSize, 1L, TimeUnit.MILLISECONDS, workQueue);

        for (int i = 0; i < 100; i++) {
            int no = i;

            executorService.execute(() -> {
                System.out.println(Thread.currentThread().getName() + " start: " + no);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + " end: " + no);
            });
        }

        executorService.shutdown();
        System.out.println("Main Thread End!");
    }
}
