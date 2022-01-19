package com.reiser.juc.thread_interaction;

/**
 * @author: reiserx
 * Date:2020/12/20
 * Des:线程间交互
 */
public class Main {
    public static void main(String[] args) {

//        TestDemo testDemo=new ThreadInteractionDemo();
//        TestDemo testDemo=new WaitDemo();
        TestDemo testDemo=new JoinDemo();
        testDemo.runTest();
    }
}
