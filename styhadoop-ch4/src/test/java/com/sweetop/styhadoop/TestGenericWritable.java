package com.sweetop.styhadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-17
 * Time: 上午9:51
 * To change this template use File | Settings | File Templates.
 */
public class TestGenericWritable {

    public static void main(String[] args) throws IOException {
        Text text=new Text("\u0041\u0071");
        MyWritable myWritable=new MyWritable(text);
        System.out.println(StringUtils.byteToHexString(SerializeUtils.serialize(text)));
        System.out.println(StringUtils.byteToHexString(SerializeUtils.serialize(myWritable)));

    }
}



