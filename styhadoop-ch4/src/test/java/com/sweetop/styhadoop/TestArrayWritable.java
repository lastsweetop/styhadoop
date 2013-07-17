package com.sweetop.styhadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-17
 * Time: 上午11:14
 * To change this template use File | Settings | File Templates.
 */
public class TestArrayWritable {
    public static void main(String[] args) throws IOException {
        ArrayWritable arrayWritable=new ArrayWritable(Text.class);
        arrayWritable.set(new Writable[]{new Text("\u0071"),new Text("\u0041")});
        System.out.println(StringUtils.byteToHexString(SerializeUtils.serialize(arrayWritable)));
    }
}
