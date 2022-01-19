package com.reiser.juc.thread_interaction;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:
 */
public class JoinDemo implements TestDemo {


    private String sharedString;

    private synchronized void initString() {
        sharedString = "Good Boy A!!!";
    }

    private synchronized void printString() {
        System.out.println("String: " + sharedString);
    }

    Thread thread;
    @Override
    public void runTest() {
        thread= new Thread() {
            @Override
            public void run() {
                super.run();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                initString();
            }
        };

        thread.start();
        new Thread(){
            @Override
            public void run() {
                super.run();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                printString();
            }
        }.start();


    }
}
