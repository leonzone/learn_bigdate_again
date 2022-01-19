package com.reiser.juc.thread_interaction;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:notifyAll 和 wait 测试
 */
public class WaitDemo implements TestDemo {


    private String sharedString;

    private synchronized void initString() {
        sharedString = "Good Boy A!!!";

        notifyAll();
    }

    private synchronized void printString() {
        // 必须使用 while，因为不知道被谁唤醒的，这是标准写法
        while (sharedString==null){
            try {
                // wait 会释放 monitor，所以 wait 必须要放在 synchronized 内
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                printString();
            }
        }.start();


    }
}
