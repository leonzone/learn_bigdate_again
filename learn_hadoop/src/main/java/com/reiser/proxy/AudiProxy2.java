package com.reiser.proxy;

/**
 * @author: reiserx
 * Date:2021/7/25
 * Des:静态代理模式，有时原始类没有接口或没有暴露，则使用继承的方式
 */
public class AudiProxy2 extends Audi {

    @Override
    public void drive() {
        System.out.println("Proxy2:检查汽车、系好安全带、打火");
        super.drive();
        System.out.println("Proxy2:发朋友圈");
    }

    public static void main(String[] args) {
        Car car = new AudiProxy2();
        car.drive();
    }
}
