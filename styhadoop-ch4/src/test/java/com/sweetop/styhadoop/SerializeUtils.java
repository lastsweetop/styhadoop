package com.sweetop.styhadoop;

import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-8
 * Time: 下午7:18
 * To change this template use File | Settings | File Templates.
 */
public class SerializeUtils {

    /**
     * 将一个实现了Writable接口的对象序列化成字节流
     * @param writable
     * @return
     * @throws java.io.IOException
     */
    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    /**
     * 将字节流转化为实现了Writable接口的对象
     * @param writable
     * @param bytes
     * @return
     * @throws IOException
     */
    public static byte[] deserialize(Writable writable,byte[] bytes) throws IOException {
        ByteArrayInputStream in=new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }
}
