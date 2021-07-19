package com.reiser.rpc;

/**
 * @author: reiserx
 * Date:2021/7/20
 * Des: 通讯协议
 */
public interface ClientProtocol {
    long versionID = 1L;

    Message findName(String studentId);
}
