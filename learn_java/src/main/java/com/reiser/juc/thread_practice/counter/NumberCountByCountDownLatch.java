package com.reiser.juc.thread_practice.counter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author: reiserx
 * Date:2021/3/3
 * Des:CountDownLatch 多线程求和
 */
public class NumberCountByCountDownLatch {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int num = 10;
        CountDownLatch latch = new CountDownLatch(num);

        int start = 0;
        int end = 0;

        List<Future<Integer>> list = new ArrayList<>();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        for (int i = 0; i < num; i++) {
            start = i * 100 + 1;
            end = start + 99;
            CountDownLatchTask task = new CountDownLatchTask(start, end, latch);
            Future<Integer> futureTask = threadPool.submit(task);
            list.add(futureTask);
        }
        System.out.println("各任务开始");
        // 等待到 CountDownLatch num减为0
        latch.await();
        int result = 0;

        for (Future<Integer> future : list) {
            result += future.get();
        }


        System.out.println("全部任务完成,结果为：" + result);
        threadPool.shutdown();


    }

}

class CountDownLatchTask implements Callable<Integer> {
    CountDownLatch latch;
    private int result = 0;
    private final int start;
    private final int end;

    public CountDownLatchTask(int start, int end, CountDownLatch latch) {
        this.start = start;
        this.end = end;
        this.latch = latch;
    }

    @Override
    public Integer call() throws Exception {
        try {

            int i = start;
            for (; i <= end; i++) {
                this.result += i;
            }
            Thread.sleep(new Random().nextInt(1000));
            System.out.println("任务完成：" + Thread.currentThread().getName() + " value: " + result);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
        return result;
    }
}
