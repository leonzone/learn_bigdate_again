package com.reiser.juc.thread_practice.counter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author: reiserx
 * Date:2021/3/3
 * Des:CyclicBarrier(循环屏障), 可以让一组线程等待满足某个条件后同时执行。
 */
public class CountWithCyclicBarrier {

    public static void main(String[] args) {
        final List<Worker> list = new ArrayList<Worker>();
        CyclicBarrier barrier = new CyclicBarrier(10, new Runnable() {
            @Override
            public void run() {
                int result = 0;
                for (Worker worker : list) {
                    result += worker.getResult();
                }
                System.out.println("计算完成，结果为" + result);
            }
        });

        int start = 0;
        int end = 0;
        Worker worker = null;
        for (int i = 0; i < 10; i++) {
            start = i * 100 + 1;
            end = start + 99;
            worker = new Worker(start, end, barrier);
            new Thread(worker).start();
            list.add(worker);
        }
    }

}

class Worker implements Runnable {

    private int result = 0;
    private final int start;
    private final int end;
    private CyclicBarrier barrier;

    public Worker(int start, int end, CyclicBarrier barrier) {
        this.start = start;
        this.end = end;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        int i = start;
        for (; i <= end; i++) {
            this.result += i;
        }
        try {
            barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    public int getResult() {
        return this.result;
    }
}
