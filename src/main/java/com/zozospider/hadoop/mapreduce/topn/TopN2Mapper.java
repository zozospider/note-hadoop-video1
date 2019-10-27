package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Mapper
 */
public class TopN2Mapper extends Mapper<LongWritable, Text, TopN2KeyWritable, Text> {

    private TreeMap<TopN2KeyWritable, Text> map = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc 10 15

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] fields = line.split(" ");

        TopN2KeyWritable k = new TopN2KeyWritable();
        Text v = new Text();
        k.set(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]));
        v.set(fields[0]);

        // k: TopN1KeyWritable{field1=10, field2=15, fieldSum=25}
        // v: abc

        // 添加数据
        if (map.size() <= 8) {
            map.put(k, v);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // 写出
        Set<Map.Entry<TopN2KeyWritable, Text>> entries = map.entrySet();
        for (Map.Entry<TopN2KeyWritable, Text> entry : entries) {
            context.write(entry.getKey(), entry.getValue());
        }
    }

}
