package com.reiser.rpc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: reiserx
 * Date:2021/7/20
 * Des: 返回的信息
 */

public class Message implements Writable {
    public Text message;
    public LongWritable code;

    public Message() {
        code = new LongWritable();
        message = new Text();
    }

    public Message(long id, String msg) {
        code = new LongWritable(id);
        message = new Text(msg);
    }

    public Text getMessage() {
        return message;
    }

    public void setMessage(Text message) {
        this.message = message;
    }

    public LongWritable getCode() {
        return code;
    }

    public void setCode(LongWritable code) {
        this.code = code;
    }

    public void write(DataOutput dataOutput) throws IOException {
        code.write(dataOutput);
        message.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        code.readFields(dataInput);
        message.readFields(dataInput);
    }
}
