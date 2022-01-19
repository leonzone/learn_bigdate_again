package com.reiser.juc.thread_synchronization;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:锁和读写锁
 */
public class LockDemo {
    private int x = 0;
    private int y = 0;
    private Lock lock = new ReentrantLock();

    private ReentrantReadWriteLock lock1 = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.ReadLock readLock = lock1.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock1.writeLock();

    private void reset() {
        lock.lock();
        try {
            x = 0;
            y = 0;
        } finally {
            // 很麻烦，不如直接使用 synchronized,通常使用读写锁
            lock.unlock();
        }
    }

    // 读写锁的粒度更细，可以共同读，但是读的时候不能写
    private void count(int newValue) {
        writeLock.lock();
        try {
            x = newValue;
            y = newValue;
        } finally {
            writeLock.unlock();
        }

    }

    private void print() {
        readLock.lock();
        try {
            System.out.println("x: " + x + ",y: " + y);
        } finally {
            readLock.unlock();
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
    }
}
