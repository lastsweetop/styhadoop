package com.sweetop.styhadoop;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.*;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lastsweetop
 * Date: 13-9-3
 * Time: 下午7:07
 * To change this template use File | Settings | File Templates.
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {

    public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"WeatherRecord\",\"doc\":\"A weather reading.\",\"fields\":[{\"name\":\"year\",\"type\":\"int\"},{\"name\":\"temperature\",\"type\":\"int\"},{\"name\":\"stationId\",\"type\":\"string\"}]}");

    public static class MaxTemperatureMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private GenericRecord record = new GenericData.Record(SCHEMA);

        @Override
        public void map(Utf8 line, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) throws IOException {
            parser.parse(line.toString());
            if (parser.isValidTemperature()) {
                record.put("year", parser.getYearInt());
                record.put("temperature", parser.getAirTemperature());
                record.put("stationId", parser.getStationId());
                collector.collect(new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
            }
        }
    }

    public static class MaxTemperatureReducer extends AvroReducer<Integer, GenericRecord, GenericRecord> {
        @Override
        public void reduce(Integer key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {
            GenericRecord max = null;
            for (GenericRecord value : values) {
                if (max == null || (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
                    max = newTemperatureRecord(value);
                }
            }
            collector.collect(max);
        }
    }

    private static GenericRecord newTemperatureRecord(GenericRecord value) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("year", value.get("year"));
        record.put("temperature", value.get("temperature"));
        record.put("stationId", value.get("stationId"));
        return record;
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output> \n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
        }
        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Max Temperature");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
        AvroJob.setOutputSchema(conf, SCHEMA);

        conf.setInputFormat(AvroUtf8InputFormat.class);

        AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
        AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

        JobClient.runJob(conf);

        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
        System.exit(exitCode);
    }
}
