package com.reiser.proxy;

/**
 * @author: reiserx
 * Date:2021/7/25
 * Des:静态代理模式
 */
public class AudiProxy implements Car {
    Audi audi;

    public AudiProxy(Audi audi) {
        this.audi = audi;
    }

    @Override
    public void drive() {
        System.out.println("Proxy:检查汽车、系好安全带、打火");
        audi.drive();
        System.out.println("Proxy:发朋友圈");
    }

    public static void main(String[] args) {
        Car car = new AudiProxy(new Audi());
        car.drive();
    }
}
