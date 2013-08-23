package com.sweetop.styhadoop;

import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-8-8
 * Time: 上午9:45
 * To change this template use File | Settings | File Templates.
 */
public class AvroDataFile {
    @Test
    public void test() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("/StringPair.avsc"));

        //根据schema创建一个record示例
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        //首先创建一个扩展名为avro的文件（扩展名随意，这里只是为了容易分辨）
        File file = new File("data.avro");
        //这行和前篇文章的代码一致，创建一个Generic Record的datum writer
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        //和Encoder不同，DataFileWriter可以将avro数据写入到文件中
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        //创建文件，并且写入头信息
        dataFileWriter.create(schema,file);
        //写datum数据
        dataFileWriter.append(datum);
        dataFileWriter.append(datum);
        dataFileWriter.close();

        //这行也和前篇文章相同，Generic Record的datum读取类，有点不一样的就是这里不需要再传入schema，因为schema已经包含在datafile的头信息里
        DatumReader<GenericRecord> reader=new GenericDatumReader<GenericRecord>();
        //datafile文件的读取类，指定文件和datumreader
        DataFileReader<GenericRecord> dataFileReader=new DataFileReader<GenericRecord>(file,reader);
        //测试下读写的schema是否一致
        Assert.assertEquals(schema,dataFileReader.getSchema());
        //遍历GenericRecord
        for (GenericRecord record : dataFileReader){
            System.out.println("left="+record.get("left")+",right="+record.get("right"));
        }
    }
}
