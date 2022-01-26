package com.reiser.juc.thread_practice.counter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author: reiserx
 * Date:2021/3/3
 * Des:CyclicBarrier(循环屏障), 可以让一组线程等待满足某个条件后同时执行。
 */
public class NumberCountByCyclicBarrier {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //为什么大了就无法跑了？
        int num = 10;
        final List<Counter> list = new ArrayList<>();
        CyclicBarrier barrier = new CyclicBarrier(num, new Runnable() {
            @Override
            public void run() {

                int result = 0;
                for (Counter counter : list) {
                    result += counter.getResult();
                }
                System.out.println("全部任务完成,结果为："+result);

            }
        });

        int start = 0;
        int end = 0;

        ExecutorService executor = Executors.newCachedThreadPool();

        for (int i = 0; i < num; i++) {
            start = i * 100 + 1;
            end = start + 99;
            Counter task = new Counter(start, end, barrier);
            executor.submit(task);
            list.add(task);
        }

//        executor.shutdown();
        System.out.println("主程序结束");


    }

}

class Counter implements Runnable {
    CyclicBarrier barrier;
    private int result = 0;
    private final int start;
    private final int end;

    public Counter(int start, int end, CyclicBarrier barrier) {
        this.start = start;
        this.end = end;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        try {

            int i = start;
            for (; i <= end; i++) {
                this.result += i;
            }
            Thread.sleep(new Random().nextInt(1000));
            System.out.println("任务完成：" + Thread.currentThread().getName() + " value: " + result);
            barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    public int getResult() {
        return result;
    }
}
