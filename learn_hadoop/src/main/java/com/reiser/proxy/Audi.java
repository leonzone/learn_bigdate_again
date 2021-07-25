package com.reiser.proxy;

/**
 * @author: reiserx
 * Date:2021/7/25
 * Des:原始类
 */
public class Audi implements Car{
    @Override
    public void drive() {
        System.out.println("行驶中:骑上我心爱的小奥迪...");
    }
}
