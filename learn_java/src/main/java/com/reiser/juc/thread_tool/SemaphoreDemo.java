package com.reiser.juc.thread_tool;

import java.util.concurrent.Semaphore;

/**
 * @author: reiserx
 * Date:2021/3/6
 * Des: Semaphore
 */
public class SemaphoreDemo {

    public static void main(String[] args) {

        //Semaphore 的 permits 相当于是整个资源，道路的宽度。如果等于1相当于一把独占锁
        Semaphore semaphore = new Semaphore(3);

        int workerNumber = 8;

        for (int i = 0; i < workerNumber; i++) {
            // run 不会启新线程
//            new Worker(i,semaphore).run();
            Worker worker = new Worker(i, semaphore);
            worker.start();
            //连续执行会抛出  IllegalThreadStateException
//            worker.start();

        }

    }

    static class Worker extends Thread {
        private int num;
        private Semaphore semaphore;

        public Worker(int num, Semaphore semaphore) {
            this.num = num;
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            try {
//                semaphore.acquire();
                semaphore.acquire(1);
                System.out.println("工人" + this.num + "占用一个机器在生产..." + Thread.currentThread().getName());
                Thread.sleep(2000);
                System.out.println("工人" + this.num + "释放出机器");
//                semaphore.release();
                semaphore.release(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
