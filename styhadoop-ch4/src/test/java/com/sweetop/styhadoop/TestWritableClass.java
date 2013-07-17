package com.sweetop.styhadoop;

import junit.framework.Assert;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-8
 * Time: 下午7:16
 * To change this template use File | Settings | File Templates.
 */
public class TestWritableClass {
    @Test
    public void testVIntWritable() throws IOException {
        byte[] data = SerializeUtils.serialize(new VIntWritable(163));
        Assert.assertEquals(StringUtils.byteToHexString(data), "8fa3");
    }

    @Test
    public void testTextIndex() {
        Text text = new Text("hadoop");
        Assert.assertEquals(text.getLength(), 6);
        Assert.assertEquals(text.getBytes().length, 6);
        Assert.assertEquals(text.charAt(2), (int) 'd');
        Assert.assertEquals("Out of bounds", text.charAt(100), -1);
    }

    @Test
    public void testTextFind() {
        Text text = new Text("hadoop");
        Assert.assertEquals("find a substring", text.find("do"), 2);
        Assert.assertEquals("Find first 'o'", text.find("o"), 3);
        Assert.assertEquals("Find 'o' from position 4 or later", text.find("o", 4), 4);
        Assert.assertEquals("No match", text.find("pig"), -1);
    }

    @Test
    public void string() throws UnsupportedEncodingException {
        String str = "\u0041\u00DF\u6771\uD801\uDC00";
        Assert.assertEquals(str.length(), 5);
        Assert.assertEquals(str.getBytes("UTF-8").length, 10);

        Assert.assertEquals(str.indexOf("\u0041"), 0);
        Assert.assertEquals(str.indexOf("\u00DF"), 1);
        Assert.assertEquals(str.indexOf("\u6771"), 2);
        Assert.assertEquals(str.indexOf("\uD801\uDC00"), 3);

        Assert.assertEquals(str.charAt(0), '\u0041');
        Assert.assertEquals(str.charAt(1), '\u00DF');
        Assert.assertEquals(str.charAt(2), '\u6771');
        Assert.assertEquals(str.charAt(3), '\uD801');
        Assert.assertEquals(str.charAt(4), '\uDC00');

        Assert.assertEquals(str.codePointAt(0), 0x0041);
        Assert.assertEquals(str.codePointAt(1), 0x00DF);
        Assert.assertEquals(str.codePointAt(2), 0x6771);
        Assert.assertEquals(str.codePointAt(3), 0x10400);
    }

    @Test
    public void text() {
        Text text = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        Assert.assertEquals(text.getLength(), 10);

        Assert.assertEquals(text.find("\u0041"), 0);
        Assert.assertEquals(text.find("\u00DF"), 1);
        Assert.assertEquals(text.find("\u6771"), 3);
        Assert.assertEquals(text.find("\uD801\uDC00"), 6);

        Assert.assertEquals(text.charAt(0), 0x0041);
        Assert.assertEquals(text.charAt(1), 0x00DF);
        Assert.assertEquals(text.charAt(3), 0x6771);
        Assert.assertEquals(text.charAt(6), 0x10400);

    }

    @Test
    public void testTextMutability() {
        Text text = new Text("hadoop");
        text.set("pig");
        Assert.assertEquals(text.getLength(), 3);
        Assert.assertEquals(text.getBytes().length, 3);
    }

    @Test
    public void testTextMutability2() {
        Text text = new Text("hadoop");
        text.set(new Text("pig"));
        Assert.assertEquals(text.getLength(),3);
        Assert.assertEquals(text.getBytes().length,6);
    }
}

