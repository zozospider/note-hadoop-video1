package com.zozospider.hadoop.mapreduce.flowCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map 阶段
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowCountValueWritable> {

    Text keyOut = new Text();
    FlowCountValueWritable valueOut = new FlowCountValueWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: 1|13366999900|192.168.1.0|Mi8SE|www.meituan.com|5636|7788|200

        // 1 获取 1 行
        String line = value.toString();

        // 2 切割
        String[] fields = line.split("\\|");
        keyOut.set(fields[1]);
        valueOut.set(Long.parseLong(fields[fields.length - 3]), Long.parseLong(fields[fields.length - 2]));

        // key_out: 13366999900
        // value_out: FlowCountValueWritable{upFlow=5636, downFlow=7788, sumFlow=13424}

        // 4 Map 写出
        context.write(keyOut, valueOut);
    }

}
