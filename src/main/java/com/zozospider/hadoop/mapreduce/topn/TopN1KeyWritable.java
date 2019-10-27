package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现 Writable 接口
 */
public class TopN1KeyWritable implements WritableComparable<TopN1KeyWritable> {

    private int field1;
    private int field2;
    // fieldSum = field1 + field2
    private int fieldSum;

    public TopN1KeyWritable() {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(field1);
        out.writeInt(field2);
        out.writeInt(fieldSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.field1 = in.readInt();
        this.field2 = in.readInt();
        this.fieldSum = in.readInt();
    }

    @Override
    public int compareTo(TopN1KeyWritable o) {
        return 0;
    }

}
