package com.reiser.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2021/7/20
 * Des: 服务端
 */
public class RPCServer implements ClientProtocol {
    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8898)
                .setProtocol(ClientProtocol.class)
                .setInstance(new RPCServer())
                .build();
        System.out.println("RPCServer start....");
        server.start();
    }

    @Override
    public Message findName(String studentId) {
        Message msg;
        switch (studentId) {
            case "20210123456789":
                msg = new Message(200, "大心心");
                break;
            default:
                msg = new Message(404, "null");
        }
        return msg;
    }
}
