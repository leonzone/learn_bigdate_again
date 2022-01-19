package com.reiser.juc.thread_synchronization;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:synchronized 测试
 */
public class Synchronized1Demo {

    private int x = 0;
    private int y = 0;
//    如果不加 会导致 x y 的值不一致
//    private void count(int newValue) {
//        x = newValue;
//        y = newValue;
//
//        if (x != y) {
//            System.out.println("x: " + x + ",y: " + y);
//        }
//    }

//    //写法一：此时 monitor 为 this
//    private synchronized void count(int newValue) {
//        x = newValue;
//        y = newValue;
//
//        if (x != y) {
//            System.out.println("x: " + x + ",y: " + y);
//        }
//    }

    //写法二：此时 monitor 为 this， 和写法一效果完全一样
    private void count(int newValue) {
        synchronized (this) {
            x = newValue;
            y = newValue;

            if (x != y) {
                System.out.println("x: " + x + ",y: " + y);
            }
        }
    }
    // 写法三，可以针对不同的方法加不同的 monitor, 只要 monitor 的那个对象不同

    public void runTest() {

        new Thread() {
            @Override
            public void run() {
                super.run();
                for (int i = 0; i < 1_000_000_000; i++) {
                    count(i);
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                super.run();
                for (int i = 0; i < 1_000_000_000; i++) {
                    count(i);
                }
            }
        }.start();

    }
}
