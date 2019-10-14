package com.zozospider.hadoop.mapreduce.join.mapjoin;

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
public class MapJoinReducer extends Reducer<IntWritable, MapJoinValueWritable, Text, NullWritable> {

    private Text keyOut = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<MapJoinValueWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: 3
        // value_in: [MapJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=},
        //            MapJoinValueWritable{flag=fa, aId=7, aName=David, bId=3, bName=},
        //            MapJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}]

        // 存储输入 value 为 fa 类型的多个 MapJoinValueWritable 对象
        List<MapJoinValueWritable> faValueIns = new ArrayList<>();
        // 存储输入 value 为 fb 类型的 1 个 MapJoinValueWritable 对象
        MapJoinValueWritable fbValueIn = new MapJoinValueWritable();

        for (MapJoinValueWritable value : values) {

            if ("fa".equals(value.getFlag())) {
                // value_in: MapJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=}

                try {
                    // 将当前循环中的 value 对象拷贝到 faValueIn 对象中 (不直接引用是为了防止循环导致 value 指针变化问题), 然后添加到 faValueIns 列表中
                    MapJoinValueWritable faValueIn = new MapJoinValueWritable();
                    BeanUtils.copyProperties(faValueIn, value);
                    faValueIns.add(faValueIn);

                    // faValueIn: MapJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=}

                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }

            } else if ("fb".equals(value.getFlag())) {
                // value_in: MapJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}

                try {
                    // 将当前循环中的 value 对象拷贝到 fbValueIn 对象中 (不直接引用是为了防止循环导致 value 指针变化问题).
                    BeanUtils.copyProperties(fbValueIn, value);

                    // fbValueIn: MapJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}

                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // faValueIns: [MapJoinValueWritable{flag=fa, aId=1, aName=Frank, bId=3, bName=},
        //              MapJoinValueWritable{flag=fa, aId=7, aName=David, bId=3, bName=}]

        // fbValueIn: MapJoinValueWritable{flag=fb, aId=0, aName=, bId=3, bName=Chicago}

        for (MapJoinValueWritable faValueIn : faValueIns) {
            keyOut.set("aId: " + faValueIn.getaId() + ", aName: " + faValueIn.getaName() + ", bId: " + faValueIn.getbId() + ", bName: " + fbValueIn.getbName());
            context.write(keyOut, NullWritable.get());
        }

        // aId: 1, aName: Frank, bId: 3, bName: Chicago
        // aId: 7, aName: David, bId: 3, bName: Chicago
    }

}
