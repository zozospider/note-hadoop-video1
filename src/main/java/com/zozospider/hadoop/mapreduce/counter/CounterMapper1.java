package com.zozospider.hadoop.mapreduce.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class CounterMapper1 extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc abc love

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] fields = line.split(" ");

        // 只写出合法字段数, 并分别统计合法与不合法的累计行数
        if (fields.length == 3) {
            // 写出
            context.write(value, NullWritable.get());
            context.getCounter("fields_legal", "true").increment(1);
        } else {
            context.getCounter("fields_legal", "false").increment(1);
        }
    }

}
