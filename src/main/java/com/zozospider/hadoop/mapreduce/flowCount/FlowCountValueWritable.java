package com.zozospider.hadoop.mapreduce.flowCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现 Writable 接口
 */
public class FlowCountValueWritable implements Writable {

    // 上行流量
    private long upFlow;
    // 下行流量
    private long downFlow;
    // 总流量
    private long sumFlow;

    /**
     * 空构造方法, 必须实现
     */
    public FlowCountValueWritable() {
        super();
    }

    /**
     * 序列化
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化 (和序列化顺序一致)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    /**
     * 最终结果输出到文件中的格式
     */
    @Override
    public String toString() {
        return "FlowCountValueWritable{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    /**
     * 设置流量
     *
     * @param upFlow   上行流量
     * @param downFlow 下行流量
     */
    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 如果需要将自定义的 bean 放在 key 中传输, 需要实现 `Comparable` 接口
     * @param o 被比较的对象
     * @return 比较结果
     */
    /*@Override
    public int compareTo(FlowCountValueWritable o) {
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }*/

}
