package com.zozospider.hadoop.mapreduce.join.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer
 */
public class ReduceJoinReducer extends Reducer<IntWritable, ReduceJoinValueWritable, Text, NullWritable> {

    private Text keyOut = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<ReduceJoinValueWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: 3
        // value_in: [ReduceJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=},
        //            ReduceJoinValueWritable{flag=fa, aId=7, aName=David, bId=3, bName=},
        //            ReduceJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}]

        // 存储输入 value 为 fa 类型的多个 ReduceJoinValueWritable 对象
        List<ReduceJoinValueWritable> faValueIns = new ArrayList<>();
        // 存储输入 value 为 fb 类型的 1 个 ReduceJoinValueWritable 对象
        ReduceJoinValueWritable fbValueIn = new ReduceJoinValueWritable();

        for (ReduceJoinValueWritable value : values) {

            if ("fa".equals(value.getFlag())) {
                // value_in: ReduceJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=}

                try {
                    // 将当前循环中的 value 对象拷贝到 faValueIn 对象中 (不直接引用是为了防止循环导致 value 指针变化问题), 然后添加到 faValueIns 列表中
                    ReduceJoinValueWritable faValueIn = new ReduceJoinValueWritable();
                    BeanUtils.copyProperties(faValueIn, value);
                    faValueIns.add(faValueIn);

                    // faValueIn: ReduceJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=}

                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }

            } else if ("fb".equals(value.getFlag())) {
                // value_in: ReduceJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}

                try {
                    // 将当前循环中的 value 对象拷贝到 fbValueIn 对象中 (不直接引用是为了防止循环导致 value 指针变化问题).
                    BeanUtils.copyProperties(fbValueIn, value);

                    // fbValueIn: ReduceJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}

                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // faValueIns: [ReduceJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=},
        //              ReduceJoinValueWritable{flag=fa, aId=7, aName=David, bId=3, bName=}]

        // fbValueIn: ReduceJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}

        // 循环 faValueIns, 并从 fbValueIn 中获取 bId 对应的 bName, 输出拼装结果
        // keyOut: aId: 1, aName: Frank, bId: 3, bName: Chicago
        // keyOut: aId: 7, aName: David, bId: 3, bName: Chicago
        for (ReduceJoinValueWritable faValueIn : faValueIns) {
            keyOut.set("aId: " + faValueIn.getaId() + ", aName: " + faValueIn.getaName() + ", bId: " + faValueIn.getbId() + ", bName: " + fbValueIn.getbName());
            context.write(keyOut, NullWritable.get());
        }
    }

}
