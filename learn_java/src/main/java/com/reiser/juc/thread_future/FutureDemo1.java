package com.reiser.juc.thread_future;

import java.util.Random;
import java.util.concurrent.*;

/**
 * @author: reiserx
 * Date:2021/3/6
 * Des: Future 可以等待线程的结果返回
 */
public class FutureDemo1 {

    public static void main(String[] args) {
        ExecutorService threadPool = Executors.newCachedThreadPool();

        Future<Integer> result = threadPool.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                int nextInt = new Random().nextInt(1000);
                Thread.sleep(nextInt);
                System.out.println("子线程结束");
                return nextInt;
            }
        });

        threadPool.shutdown();

        try {
            // get 会阻塞线程
            System.out.println("result: "+result.get());
            System.out.println("主线程结束");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
