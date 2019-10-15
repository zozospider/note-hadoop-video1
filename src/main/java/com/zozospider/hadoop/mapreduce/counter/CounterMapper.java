package com.zozospider.hadoop.mapreduce.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class CounterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc abc love

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] fields = line.split(" ");

        // 打印出统计的每 1 行字段数
        context.getCounter("fields_length", "len-" + fields.length).increment(1);

        // 写出
        context.write(value, NullWritable.get());
    }

}
