package com.sweetop.styhadoop;


import junit.framework.Assert;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-4
 * Time: 下午10:25
 * To change this template use File | Settings | File Templates.
 */
public class TestWritable {
    byte[] bytes=null;


    /**
     * 初始化一个IntWritable实例，并且调用系列化方法
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        IntWritable writable = new IntWritable(163);
        bytes = SerializeUtils.serialize(writable);
    }


    /**
     * 一个IntWritable序列号后的四个字节的字节流
     * 并且使用big-endian的队列排列
     * @throws IOException
     */
    @Test
    public void testSerialize() throws IOException {
        Assert.assertEquals(bytes.length,4);
        Assert.assertEquals(StringUtils.byteToHexString(bytes),"000000a3");
    }

    /**
     * 创建一个没有值的IntWritable对象，并且通过调用反序列化方法将bytes的数据读入到它里面
     * 通过调用它的get方法，获得原始的值，163
     */
    @Test
    public void testDeserialize() throws IOException {
        IntWritable newWritable = new IntWritable();
        SerializeUtils.deserialize(newWritable, bytes);
        Assert.assertEquals(newWritable.get(),163);
    }

}
