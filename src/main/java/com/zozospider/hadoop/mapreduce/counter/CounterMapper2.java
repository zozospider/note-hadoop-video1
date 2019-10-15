package com.zozospider.hadoop.mapreduce.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class CounterMapper2 extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc abc love

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] fields = line.split(" ");

        // 统计不同字段数的累计行数
        context.getCounter("fields_length", "len-" + fields.length).increment(1);

        // 因为 keyOut 和 valueOut 类型为 NullWritable, 所以不需要写出
    }

}
