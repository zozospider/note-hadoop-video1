package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class MultiJob2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text keyOut = new Text();
    private IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc	f3,12|f3,8|f1,20|f1,8|f1,0|f2,16

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] words = line.split("\t");

        // 循环写出
        for (String kv : words[1].split("\\|")) {
            String k = kv.split(",")[0];
            String v = kv.split(",")[1];

            keyOut.set(words[0] + ":" + k);
            valueOut.set(Integer.valueOf(v));

            // key_out: abc:f3
            // value_out: 12
            // key_out: abc:f3
            // value_out: 8
            // key_out: abc:f1
            // value_out: 20
            // key_out: abc:f1
            // value_out: 8
            // key_out: abc:f1
            // value_out: 0
            // key_out: abc:f2
            // value_out: 16

            // 写出
            context.write(keyOut, valueOut);
        }
    }

}
