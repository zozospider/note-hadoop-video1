package com.zozospider.hadoop.mapreduce.compress.reduceout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 统计单次出现次数 (Map 端输出使用压缩方式, Reduce 端输出使用压缩方式)
 */
public class ReduceOutDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/compress/ReduceOut/
     * total 8
     * -rw-r--r--  1 user  staff    79B 10 17 21:39 f1
     * ➜  input cat compress/ReduceOut/f1
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/compress/ReduceOut/
     * ls: /Users/user/other/tmp/MapReduce/output/compress/ReduceOut/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/compress/ReduceOut", "/Users/user/other/tmp/MapReduce/output/compress/ReduceOut"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        // 开启 Map 端输出压缩, 压缩方式为 GzipCodec
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(ReduceOutDriver.class);
        job.setMapperClass(ReduceOutMapper.class);
        job.setReducerClass(ReduceOutReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 开启 Reduce 端输出压缩, 压缩方式为 Gzip
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/compress/ReduceOut/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 17 21:44 _SUCCESS
     * -rw-r--r--  1 user  staff    83B 10 17 21:44 part-r-00000.gz
     * ➜  output
     */

    /**
     * 2019-10-17 21:44:29,243 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-17 21:44:29,603 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-17 21:44:29,611 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-17 21:44:30,116 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-17 21:44:30,122 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-17 21:44:30,146 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-17 21:44:30,200 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-17 21:44:30,321 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local421934656_0001
     * 2019-10-17 21:44:30,552 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-17 21:44:30,552 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-17 21:44:30,553 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local421934656_0001
     * 2019-10-17 21:44:30,559 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-17 21:44:30,560 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-17 21:44:30,612 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-17 21:44:30,612 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local421934656_0001_m_000000_0
     * 2019-10-17 21:44:30,644 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-17 21:44:30,652 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-17 21:44:30,653 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-17 21:44:30,659 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/compress/ReduceOut/f1:0+79
     * 2019-10-17 21:44:30,737 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-17 21:44:30,737 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-17 21:44:30,738 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-17 21:44:30,738 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-17 21:44:30,738 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-17 21:44:30,740 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-17 21:44:30,749 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-17 21:44:30,749 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-17 21:44:30,749 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-17 21:44:30,749 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 143; bufvoid = 104857600
     * 2019-10-17 21:44:30,750 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214336(104857344); length = 61/6553600
     * 2019-10-17 21:44:30,761 INFO [org.apache.hadoop.io.compress.CodecPool] - Got brand-new compressor [.bz2]
     * 2019-10-17 21:44:30,779 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-17 21:44:30,784 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local421934656_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-17 21:44:30,792 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-17 21:44:30,793 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local421934656_0001_m_000000_0' done.
     * 2019-10-17 21:44:30,793 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local421934656_0001_m_000000_0
     * 2019-10-17 21:44:30,793 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-17 21:44:30,797 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-17 21:44:30,805 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local421934656_0001_r_000000_0
     * 2019-10-17 21:44:30,811 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-17 21:44:30,812 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-17 21:44:30,812 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-17 21:44:30,816 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@f922e47
     * 2019-10-17 21:44:30,827 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-17 21:44:30,830 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local421934656_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-17 21:44:30,874 INFO [org.apache.hadoop.io.compress.CodecPool] - Got brand-new decompressor [.bz2]
     * 2019-10-17 21:44:30,874 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local421934656_0001_m_000000_0 decomp: 177 len: 121 to MEMORY
     * 2019-10-17 21:44:30,894 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 177 bytes from map-output for attempt_local421934656_0001_m_000000_0
     * 2019-10-17 21:44:30,895 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 177, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->177
     * 2019-10-17 21:44:30,896 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-17 21:44:30,897 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-17 21:44:30,898 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-17 21:44:30,906 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-17 21:44:30,907 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 171 bytes
     * 2019-10-17 21:44:30,917 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 177 bytes to disk to satisfy reduce memory limit
     * 2019-10-17 21:44:30,917 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 135 bytes from disk
     * 2019-10-17 21:44:30,918 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-17 21:44:30,918 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-17 21:44:30,920 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 171 bytes
     * 2019-10-17 21:44:30,921 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-17 21:44:30,938 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-17 21:44:30,948 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local421934656_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-17 21:44:30,949 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-17 21:44:30,949 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local421934656_0001_r_000000_0 is allowed to commit now
     * 2019-10-17 21:44:30,951 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local421934656_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/compress/ReduceOut/_temporary/0/task_local421934656_0001_r_000000
     * 2019-10-17 21:44:30,952 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-17 21:44:30,952 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local421934656_0001_r_000000_0' done.
     * 2019-10-17 21:44:30,952 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local421934656_0001_r_000000_0
     * 2019-10-17 21:44:30,952 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-17 21:44:31,557 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local421934656_0001 running in uber mode : false
     * 2019-10-17 21:44:31,558 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-17 21:44:31,559 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local421934656_0001 completed successfully
     * 2019-10-17 21:44:31,569 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=812
     * 		FILE: Number of bytes written=552930
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=6
     * 		Map output records=16
     * 		Map output bytes=143
     * 		Map output materialized bytes=121
     * 		Input split bytes=129
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=11
     * 		Reduce shuffle bytes=121
     * 		Reduce input records=16
     * 		Reduce output records=11
     * 		Spilled Records=32
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=0
     * 		Total committed heap usage (bytes)=468713472
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=79
     * 	File Output Format Counters
     * 		Bytes Written=95
     *
     * Process finished with exit code 0
     */

}
