package com.reiser.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author: reiserx
 * Date:2021/7/25
 * Des:动态代理模式
 */
public class CarDynamicProxy {
    public Car createProxy(Car car) {
        //创建handler，代理对象调用的就是里面的 invoke 方法
        CarInvocationHandler<Car> handler = new CarInvocationHandler<>(car);
        //创建代理对象
        return (Car) Proxy.newProxyInstance(Car.class.getClassLoader(), new Class<?>[]{Car.class}, handler);
    }

    public static void main(String[] args) {
        Car myCarProxy = new CarDynamicProxy().createProxy(new Audi());
        //调用代理的方法
        myCarProxy.drive();

    }


    public class CarInvocationHandler<T> implements InvocationHandler {
        // 持有的被代理对象
        T target;

        public CarInvocationHandler(T target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            //实现代理类的方法
            System.out.println("DynamicProxy:检查汽车、系好安全带、打火");
            //原始类的方法
            Object result = method.invoke(target, args);
            System.out.println("DynamicProxy:发朋友圈");
            return result;
        }
    }
}
