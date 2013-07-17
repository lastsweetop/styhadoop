package com.sweetop.styhadoop;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-17
 * Time: 上午9:14
 * To change this template use File | Settings | File Templates.
 */
public class TestObjectWritable {
    public static void main(String[] args) throws IOException {
        Text text=new Text("\u0041");
        ObjectWritable objectWritable=new ObjectWritable(text);
        System.out.println(StringUtils.byteToHexString(SerializeUtils.serialize(objectWritable)));

    }
}
