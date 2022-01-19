package com.reiser.juc.thread_interaction;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:
 */
public class ThreadInteractionDemo implements TestDemo {
    @Override
    public void runTest() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 1_000_000; i++) {
                    // 默认 thread.interrupt() 只会通知，不会打断，需要自己实现被终止
                    if (Thread.interrupted()) {
                        //收尾
                        return;
                    }

                    try {
                        // 如果是在 sleep ，调用thread.interrupt()就会被打断
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        //收尾
                        // 会重置 interrupted 为 false
                        return;
                    }
                    System.out.println("number: " + i);
                }
            }
        };

        thread.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        // 强制打断，不友好
//        thread.stop();

        // 只会通知，不会打断执行
        thread.interrupt();
    }
}
