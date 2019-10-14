package com.zozospider.hadoop.mapreduce.join.reducejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Mapper
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, IntWritable, ReduceJoinValueWritable> {

    private String splitName;
    private IntWritable keyOut = new IntWritable();
    private ReduceJoinValueWritable valueOut = new ReduceJoinValueWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取当前 Mapper 对应的 Split 分片
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        // 获取 Split 分片对应的文件名称
        splitName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // case 1:
        //   key_in: 0
        //   value_in: 1 Frank 3

        // case 2:
        //   key_in: 0
        //   value_in: 1 New York

        // 1 获取 1 行
        String line = value.toString();
        String[] fields = line.split(" ");

        // 2 判断当前 splitName, 设置对应的 keyOut 和 valueOut
        if ("fa".equals(splitName)) {

            // 封装 keyOut
            keyOut.set(Integer.parseInt(fields[2]));

            // 封装 valueOut, 如果没有需要设置为缺省值 (不能为 NULL)
            valueOut.setFlag("fa");
            valueOut.setaId(Integer.parseInt(fields[0]));
            valueOut.setaName(fields[1]);
            valueOut.setbId(Integer.parseInt(fields[2]));
            valueOut.setbName("");

            // key_out: 3
            // value_out: ReduceJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=}

        } else if ("fb".equals(splitName)) {

            // 封装 keyOut
            keyOut.set(Integer.parseInt(fields[0]));

            // 封装 valueOut, 如果没有需要设置为缺省值 (不能为 NULL)
            valueOut.setFlag("fb");
            valueOut.setaId(0);
            valueOut.setaName("");
            valueOut.setbId(Integer.parseInt(fields[0]));
            valueOut.setbName(fields[1]);

            // key_out: 1
            // value_out: ReduceJoinValueWritable{flag=fb, aId=0, aName=, bId=1, bName=New York}
        }

        // 3 写出
        context.write(keyOut, valueOut);
    }

}
