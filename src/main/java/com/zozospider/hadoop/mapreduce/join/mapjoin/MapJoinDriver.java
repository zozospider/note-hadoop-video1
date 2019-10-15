package com.zozospider.hadoop.mapreduce.join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * MapReduce 驱动: 在 fa 的每 1 行尾部添加 field3 (bId) 对应的 fb 中的 field2 (bName).
 */
public class MapJoinDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/join/MapJoin/
     * total 8
     * -rw-r--r--  1 user  staff   105B 10 15 19:54 fa
     * ➜  input cat join/MapJoin/fa
     * 1 Frank 3
     * 2 Jack 5
     * 3 John 6
     * 4 Olivia 2
     * 5 Ava 5
     * 6 Mia 1
     * 7 David 3
     * 8 Anna 4
     * 9 Lily 2
     * 10 Luke 1
     * 11 Oliver 2
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/join/MapJoinCache/
     * total 8
     * -rw-r--r--  1 user  staff    67B 10 15 19:54 fb
     * ➜  input cat join/MapJoinCache/fb
     * 1 New_York
     * 2 Los_Angeles
     * 3 Chicago
     * 4 Houston
     * 5 Dallas
     * 6 Washington
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/join/MapJoin/
     * ls: /Users/user/other/tmp/MapReduce/output/join/MapJoin/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/join/MapJoin", "/Users/user/other/tmp/MapReduce/output/join/MapJoin"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 加载本地文件到缓存中
        job.addCacheFile(new URI("file:///Users/user/other/tmp/MapReduce/input/join/MapJoinCache/fb"));

        // Map Join 不需要 Reduce 阶段, 所以将 ReduceTask 个数设置为 0
        job.setNumReduceTasks(0);

        // 2 设置 Jar, Mapper 类
        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);

        // 3 设置最终的 KEYOUT, VALUEOUT
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/join/MapJoin/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 15 20:23 _SUCCESS
     * -rw-r--r--  1 user  staff   505B 10 15 20:23 part-m-00000
     * ➜  output cat join/MapJoin/part-m-00000
     * aId: 1, aName: Frank, bId: 3, bName: Chicago
     * aId: 2, aName: Jack, bId: 5, bName: Dallas
     * aId: 3, aName: John, bId: 6, bName: Washington
     * aId: 4, aName: Olivia, bId: 2, bName: Los_Angeles
     * aId: 5, aName: Ava, bId: 5, bName: Dallas
     * aId: 6, aName: Mia, bId: 1, bName: New_York
     * aId: 7, aName: David, bId: 3, bName: Chicago
     * aId: 8, aName: Anna, bId: 4, bName: Houston
     * aId: 9, aName: Lily, bId: 2, bName: Los_Angeles
     * aId: 10, aName: Luke, bId: 1, bName: New_York
     * aId: 11, aName: Oliver, bId: 2, bName: Los_Angeles
     * ➜  output
     */

    /**
     * 2019-10-15 20:22:58,768 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-15 20:22:59,019 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-15 20:22:59,022 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-15 20:22:59,593 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-15 20:22:59,602 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-15 20:22:59,722 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-15 20:22:59,967 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-15 20:23:00,267 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local33782329_0001
     * 2019-10-15 20:23:00,807 INFO [org.apache.hadoop.mapred.LocalDistributedCacheManager] - Creating symlink: /tmp/hadoop-user/mapred/local/1571142180532/fb <- /Volumes/DISK2/app/GitHub/note-hadoop-video1/fb
     * 2019-10-15 20:23:00,837 INFO [org.apache.hadoop.mapred.LocalDistributedCacheManager] - Localized file:/Users/user/other/tmp/MapReduce/input/join/MapJoinCache/fb as file:/tmp/hadoop-user/mapred/local/1571142180532/fb
     * 2019-10-15 20:23:00,913 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-15 20:23:00,916 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local33782329_0001
     * 2019-10-15 20:23:00,916 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-15 20:23:00,925 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 20:23:00,927 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-15 20:23:01,023 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-15 20:23:01,024 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local33782329_0001_m_000000_0
     * 2019-10-15 20:23:01,062 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 20:23:01,069 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-15 20:23:01,069 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-15 20:23:01,073 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/join/MapJoin/fa:0+105
     * fbMap: {1=New_York, 2=Los_Angeles, 3=Chicago, 4=Houston, 5=Dallas, 6=Washington}
     * 2019-10-15 20:23:01,111 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-15 20:23:01,112 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local33782329_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-15 20:23:01,118 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-15 20:23:01,118 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local33782329_0001_m_000000_0 is allowed to commit now
     * 2019-10-15 20:23:01,119 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local33782329_0001_m_000000_0' to file:/Users/user/other/tmp/MapReduce/output/join/MapJoin/_temporary/0/task_local33782329_0001_m_000000
     * 2019-10-15 20:23:01,120 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-15 20:23:01,120 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local33782329_0001_m_000000_0' done.
     * 2019-10-15 20:23:01,120 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local33782329_0001_m_000000_0
     * 2019-10-15 20:23:01,121 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-15 20:23:01,923 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local33782329_0001 running in uber mode : false
     * 2019-10-15 20:23:01,978 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-15 20:23:01,980 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local33782329_0001 completed successfully
     * 2019-10-15 20:23:02,015 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 15
     * 	File System Counters
     * 		FILE: Number of bytes read=349
     * 		FILE: Number of bytes written=275409
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=11
     * 		Map output records=11
     * 		Input split bytes=123
     * 		Spilled Records=0
     * 		Failed Shuffles=0
     * 		Merged Map outputs=0
     * 		GC time elapsed (ms)=0
     * 		Total committed heap usage (bytes)=128974848
     * 	File Input Format Counters
     * 		Bytes Read=105
     * 	File Output Format Counters
     * 		Bytes Written=517
     *
     * Process finished with exit code 0
     */

}
