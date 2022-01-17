package com.reiser.juc;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:
 */
public class Synchronized1Demo {

    private int x = 0;
    private int y = 0;

    private void count(int newValue) {
        x = newValue;
        y = newValue;

        if (x != y) {
            System.out.println("x: " + x + ",y: " + y);
        }
    }

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
