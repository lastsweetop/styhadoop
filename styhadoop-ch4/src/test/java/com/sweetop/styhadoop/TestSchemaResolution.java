package com.sweetop.styhadoop;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-8-22
 * Time: 上午11:20
 * To change this template use File | Settings | File Templates.
 */
public class TestSchemaResolution {
    @Test
    public void testAddField() throws IOException {
        //将schema从StringPair.avsc文件中加载
        Schema.Parser parser = new Schema.Parser();
        Schema newSchema = parser.parse(getClass().getResourceAsStream("/addStringPair.avsc"));

        File file = new File("data.avro");
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(null, newSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
        for (GenericRecord record : dataFileReader) {
            System.out.println("left=" + record.get("left") + ",right=" + record.get("right") + ",description="
                    + record.get("description"));
        }
    }

    @Test
    public void testRemoveField() throws IOException {
        //将schema从StringPair.avsc文件中加载
        Schema.Parser parser = new Schema.Parser();
        Schema newSchema = parser.parse(getClass().getResourceAsStream("/removeStringPair.avsc"));

        File file = new File("data.avro");
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(null, newSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
        for (GenericRecord record : dataFileReader) {
            System.out.println("left=" + record.get("left"));
        }
    }

    @Test
    public void testAliasesField() throws IOException {
        //将schema从StringPair.avsc文件中加载
        Schema.Parser parser = new Schema.Parser();
        Schema newSchema = parser.parse(getClass().getResourceAsStream("/aliasesStringPair.avsc"));

        File file = new File("data.avro");
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(null, newSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
        for (GenericRecord record : dataFileReader) {
            System.out.println("first=" + record.get("first")+",second="+record.get("second"));
        }
    }
}
