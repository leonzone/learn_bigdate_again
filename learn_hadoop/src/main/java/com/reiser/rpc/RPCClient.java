package com.reiser.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author: reiserx
 * Date:2021/7/20
 * Des:客户端
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {
        ClientProtocol client = RPC.getProxy(ClientProtocol.class,
                1L,
                new InetSocketAddress("localhost", 8898),
                new Configuration());

        Message callback = client.findName("20210000000000");
        System.out.println(callback.getMessage());

        Message callback2 = client.findName("20210123456789");
        System.out.println(callback2.getMessage());
    }
}
