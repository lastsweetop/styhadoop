package com.sweetop.styhadoop;

import junit.framework.Assert;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-8-6
 * Time: 下午2:19
 * To change this template use File | Settings | File Templates.
 */
public class TestSprecificMapping {
    @Test
    public void test() throws IOException {
        //因为已经生成StringPair的源代码，所以不再使用schema了，直接调用setter和getter即可
        StringPair datum=new StringPair();
        datum.setLeft("L");
        datum.setRight("R");

        ByteArrayOutputStream out=new ByteArrayOutputStream();
        //不再需要传schema了，直接用StringPair作为范型和参数，
        DatumWriter<StringPair> writer=new SpecificDatumWriter<StringPair>(StringPair.class);
        Encoder encoder= EncoderFactory.get().binaryEncoder(out,null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<StringPair> reader=new SpecificDatumReader<StringPair>(StringPair.class);
        Decoder decoder= DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        StringPair result=reader.read(null,decoder);
        Assert.assertEquals("L",result.getLeft().toString());
        Assert.assertEquals("R",result.getRight().toString());
    }
}
