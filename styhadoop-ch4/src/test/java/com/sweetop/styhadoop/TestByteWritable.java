package com.sweetop.styhadoop;

import junit.framework.Assert;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-16
 * Time: 下午8:46
 * To change this template use File | Settings | File Templates.
 */
public class TestByteWritable {
    @Test
    public void testByteWritableSerilizedFromat() throws IOException {
        BytesWritable bytesWritable=new BytesWritable(new byte[]{3,5});
        byte[] bytes=SerializeUtils.serialize(bytesWritable);
        Assert.assertEquals(StringUtils.byteToHexString(bytes),"000000020305");
        bytesWritable.setCapacity(11);
        bytesWritable.setSize(4);
        Assert.assertEquals(4,bytesWritable.getLength());
        Assert.assertEquals(11,bytesWritable.getBytes().length);
    }
}
