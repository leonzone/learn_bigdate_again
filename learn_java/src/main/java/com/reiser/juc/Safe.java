package com.reiser.juc;

/**
 * @author: reiserx
 * Date:2022/1/17
 * Des:线程安全
 */
public class Safe {
    public static void main(String[] args) {
        testSynchronized();
    }

    private int x = 0;
    private int y = 0;

    private void unsafeCount(int newValue) {
        x = newValue;
        y = newValue;

        if (x != y) {
            System.out.println("x: " + x + ", y:" + y);
        }
    }

    private static void testSynchronized() {
        new Synchronized1Demo().runTest();
    }

}
