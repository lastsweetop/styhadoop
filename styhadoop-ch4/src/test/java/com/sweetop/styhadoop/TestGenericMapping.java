package com.sweetop.styhadoop;


import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-8-5
 * Time: 下午7:59
 * To change this template use File | Settings | File Templates.
 */
public class TestGenericMapping {
    @Test
    public void test() throws IOException {
        //将schema从StringPair.avsc文件中加载
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("/StringPair.avsc"));

        //根据schema创建一个record示例
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        //DatumWriter可以将GenericRecord变成edncoder可以理解的类型
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        //encoder可以将数据写入流中，binaryEncoder第二个参数是重用的encoder，这里不重用，所用传空
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum,encoder);
        encoder.flush();
        out.close();

        DatumReader<GenericRecord> reader=new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        GenericRecord result=reader.read(null,decoder);
        Assert.assertEquals("L",result.get("left").toString());
        Assert.assertEquals("R",result.get("right").toString());
    }
}
