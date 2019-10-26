package com.zozospider.hadoop.mapreduce.flowCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class FlowCountReducer extends Reducer<Text, FlowCountValueWritable, Text, FlowCountValueWritable> {

    FlowCountValueWritable valueOut = new FlowCountValueWritable();

    @Override
    protected void reduce(Text key, Iterable<FlowCountValueWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: 13366999900
        // values_in: [FlowCountValueWritable{upFlow=5636, downFlow=7788, sumFlow=13424}, FlowCountValueWritable{upFlow=1802, downFlow=2380, sumFlow=4182}]

        // 1 累加求和
        long upFlowSum = 0;
        long downFlowSum = 0;

        for (FlowCountValueWritable value : values) {
            upFlowSum += value.getUpFlow();
            downFlowSum += value.getDownFlow();
        }
        valueOut.set(upFlowSum, downFlowSum);

        // key_out: 13366999900
        // value_out: FlowCountValueWritable{upFlow=7438, downFlow=10168, sumFlow=17606}

        // 2 写出
        context.write(key, valueOut);
    }

}
