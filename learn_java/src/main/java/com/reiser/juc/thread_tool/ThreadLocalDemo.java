package com.reiser.juc.thread_tool;

/**
 * @author: reiserx
 * Date:2021/3/6
 * Des:ThreadLocal 在每个线程单独有一份拷贝
 *
 * • 线程本地变量
 * • 场景: 每个线程一个副本
 * • 不改方法签名静默传参
 * • 及时进行清理
 */
public class ThreadLocalDemo {

    private static ThreadLocal<Integer> seqNum = ThreadLocal.withInitial(() -> 0);

    public ThreadLocal<Integer> getThreadLocal() {
        return seqNum;
    }

    public int getNextNum() {
        seqNum.set(seqNum.get() + 1);
        return seqNum.get();
    }


    public static void main(String[] args) {
        ThreadLocalDemo threadLocalMain = new ThreadLocalDemo();

        SnThread client1 = new SnThread(threadLocalMain);
        SnThread client2 = new SnThread(threadLocalMain);
        SnThread client3 = new SnThread(threadLocalMain);

        client1.start();
        client2.start();
        client3.start();
    }


    private static class SnThread extends Thread {
        private ThreadLocalDemo sn;

        public SnThread(ThreadLocalDemo sn) {
            this.sn = sn;
        }

        public void run() {
            for (int i = 0; i < 3; i++) {
                System.out.println("thread[" + Thread.currentThread().getName() + "] ---> sn [" + sn.getNextNum() + "]");
            }
            sn.getThreadLocal().remove();
        }
    }
}
