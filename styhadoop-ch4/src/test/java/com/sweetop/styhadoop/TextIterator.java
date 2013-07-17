package com.sweetop.styhadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-7-9
 * Time: 下午5:00
 * To change this template use File | Settings | File Templates.
 */
public class TextIterator {
    public static void main(String[] args) throws IOException {
        Text text = new Text("\u0041\u00DF\u6771\uD801\udc00");
        ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
        int cp;
        while (buffer.hasRemaining() && (cp = Text.bytesToCodePoint(buffer)) != -1) {
            System.out.println(Integer.toHexString(cp));
        }
        System.out.println(StringUtils.byteToHexString(SerializeUtils.serialize(text)));
    }
}
