package com.zozospider.hadoop.mapreduce.comparable.all;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class AllMapper extends Mapper<LongWritable, Text, AllKeyWritable, Text> {

    private AllKeyWritable keyOut = new AllKeyWritable();
    private Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc 10 15

        // 1 获取 1 行
        String line = value.toString();

        // 2 切割
        String[] fields = line.split(" ");
        keyOut.set(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]));
        valueOut.set(fields[0]);

        // key_out: AllKeyWritable{field1=10, field2=15, fieldSum=25}
        // value_out: abc

        // 4 Map 写出
        context.write(keyOut, valueOut);
    }

}
