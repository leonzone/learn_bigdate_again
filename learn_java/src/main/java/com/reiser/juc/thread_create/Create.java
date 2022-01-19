package com.reiser.juc.thread_create;

import java.util.concurrent.*;

/**
 * @author: reiserx
 * Date:2022/1/17
 * Des: 创建线程的方法
 */
public class Create {

    public static void main(String[] args) {

        // 使用 thread 类
        testThread();
        // 使用 Thread + Runnable 创建线程
        testRunable();
        // 使用 ThreadFactory
        testThreadFactory();
        // 使用 Executor 和线程池
        testExecutor();
        // Callable 和 Future
        testCallable();
        //守护线程
        testDaemonThread();
    }

    private static void testCallable() {
        Callable<String> callable = new Callable<String>() {
            @Override
            public String call() {
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return "Done!";
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<String> future = executor.submit(callable);


        try {
            String result = future.get();
            System.out.println("result: " + result);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void testExecutor() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println("Thread " + Thread.currentThread().getName() + " with Runnable started!");
//                try {
//                    Thread.sleep(500);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        };

        //常用
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(runnable);
        executor.execute(runnable);
        executor.execute(runnable);
    }

    private static void testThreadFactory() {
        ThreadFactory factory = new ThreadFactory() {

            int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                count++;
                return new Thread(r, "Thread-" + count);
            }
        };

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + " is start");
            }
        };

        Thread thread = factory.newThread(runnable);
        thread.start();

        Thread thread1 = factory.newThread(runnable);
        thread1.start();
    }

    private static void testRunable() {
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                System.out.println("Thread with Runnable is start");
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();
    }

    private static void testThread() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                super.run();
                System.out.println("new thread is start!");
            }
        };

        thread.start();
    }

    private static void testDaemonThread() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                super.run();
                System.out.println("new thread is start!");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(" thread still running!");
            }
        };
        // 其他非守护线程结束，守护线程必须同时结束
        thread.setDaemon(true);
        thread.start();
    }
}
