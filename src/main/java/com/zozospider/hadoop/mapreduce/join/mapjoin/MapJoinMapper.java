package com.zozospider.hadoop.mapreduce.join.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    // 映射本地 fb 文件内容的内存变量
    private Map<Integer, String> fbMap = new HashMap<>();
    private Text keyOut = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取第 1 个缓存文件路径
        URI[] cacheFiles = context.getCacheFiles();
        String cacheFilePath = cacheFiles[0].getPath();

        // 获取对应的 reader
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFilePath), "UTF-8"));

        // 循环读取本地文件的每 1 行内容, 加入到内存变量中
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // line: 1 New_York

            String[] fields = line.split(" ");
            fbMap.put(Integer.valueOf(fields[0]), fields[1]);
        }

        // fbMap: {1=New_York, 2=Los_Angeles, 3=Chicago, 4=Houston, 5=Dallas, 6=Washington}

        // 关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: 1 Frank 3

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] fields = line.split(" ");

        // 获取 bId
        Integer bId = Integer.valueOf(fields[2]);

        // 从 fbMap 中获取 bId 对应的 bName
        String bName = fbMap.get(bId);

        // 输出拼装结果
        // keyOut: aId: 1, aName: Frank, bId: 3, bName: Chicago
        keyOut.set("aId: " + fields[0] + ", aName: " + fields[1] + ", bId: " + bId + ", bName: " + bName);
        context.write(keyOut, NullWritable.get());
    }

}
