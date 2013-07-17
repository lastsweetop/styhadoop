package com.sweetop.styhadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-16
 * Time: 下午9:23
 * To change this template use File | Settings | File Templates.
 */
public class TestNullWritable {
    public static void main(String[] args) throws IOException {
        NullWritable nullWritable=NullWritable.get();
        System.out.println(StringUtils.byteToHexString(SerializeUtils.serialize(nullWritable)));
    }
}
