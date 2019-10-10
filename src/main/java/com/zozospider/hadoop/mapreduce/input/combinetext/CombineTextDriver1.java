package com.zozospider.hadoop.mapreduce.input.combinetext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动
 * 参考: [Hadoop CombineTextInputFormat 切片机制](https://blog.csdn.net/yljphp/article/details/89070948)
 */
public class CombineTextDriver1 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/CombineText1/
     * total 3024
     * -rw-r--r--  1 user  staff    79B 10 10 13:53 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:53 b
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText1/
     * ls: /Users/user/other/tmp/MapReduce/output/CombineText1/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/CombineText1", "/Users/user/other/tmp/MapReduce/output/CombineText1"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextDriver1.class);
        job.setMapperClass(CombineTextMapper.class);
        job.setReducerClass(CombineTextReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText1/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 16:17 _SUCCESS
     * -rw-r--r--  1 user  staff   123B 10 10 16:17 part-r-00000
     * ➜  output cat CombineText1/part-r-00000
     * abc	39012
     * awesome	19505
     * book	39010
     * enough	19505
     * google	19505
     * love	58516
     * me	19505
     * please	19505
     * qq	39011
     * see	19505
     * who	19505
     * ➜  output
     */

    /**
     * 2019-10-10 16:16:59,231 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 16:16:59,487 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 16:16:59,489 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 16:17:00,123 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 16:17:00,128 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 16:17:00,150 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 2
     * 2019-10-10 16:17:00,169 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 1540911
     * 2019-10-10 16:17:00,217 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-10 16:17:00,323 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local2074560084_0001
     * 2019-10-10 16:17:00,541 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 16:17:00,543 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 16:17:00,544 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local2074560084_0001
     * 2019-10-10 16:17:00,549 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:00,551 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 16:17:00,600 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 16:17:00,601 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local2074560084_0001_m_000000_0
     * 2019-10-10 16:17:00,632 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:00,640 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:17:00,640 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:17:00,647 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText1/a:0+79,/Users/user/other/tmp/MapReduce/input/CombineText1/b:0+1540832
     * 2019-10-10 16:17:00,707 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:17:00,707 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:17:00,707 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:17:00,707 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:17:00,707 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:17:00,709 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:17:01,130 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:17:01,131 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:17:01,131 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:17:01,131 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 2789247; bufvoid = 104857600
     * 2019-10-10 16:17:01,131 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 24966064(99864256); length = 1248333/6553600
     * 2019-10-10 16:17:01,394 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:17:01,399 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local2074560084_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:17:01,405 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:17:01,406 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local2074560084_0001_m_000000_0' done.
     * 2019-10-10 16:17:01,406 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local2074560084_0001_m_000000_0
     * 2019-10-10 16:17:01,406 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 16:17:01,408 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 16:17:01,408 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local2074560084_0001_r_000000_0
     * 2019-10-10 16:17:01,414 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:01,414 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:17:01,414 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:17:01,416 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@14ec6ac4
     * 2019-10-10 16:17:01,427 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 16:17:01,428 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local2074560084_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 16:17:01,466 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local2074560084_0001_m_000000_0 decomp: 3413417 len: 3413421 to MEMORY
     * 2019-10-10 16:17:01,484 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 3413417 bytes from map-output for attempt_local2074560084_0001_m_000000_0
     * 2019-10-10 16:17:01,486 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 3413417, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->3413417
     * 2019-10-10 16:17:01,487 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 16:17:01,488 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 16:17:01,488 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 16:17:01,496 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:17:01,497 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 3413411 bytes
     * 2019-10-10 16:17:01,545 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local2074560084_0001 running in uber mode : false
     * 2019-10-10 16:17:01,547 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 16:17:01,677 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 3413417 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 16:17:01,677 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 3413421 bytes from disk
     * 2019-10-10 16:17:01,678 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 16:17:01,678 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:17:01,681 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 3413411 bytes
     * 2019-10-10 16:17:01,681 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 16:17:01,696 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 16:17:01,977 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local2074560084_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:17:01,980 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 16:17:01,980 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local2074560084_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 16:17:01,981 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local2074560084_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/CombineText1/_temporary/0/task_local2074560084_0001_r_000000
     * 2019-10-10 16:17:01,982 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 16:17:01,982 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local2074560084_0001_r_000000_0' done.
     * 2019-10-10 16:17:01,983 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local2074560084_0001_r_000000_0
     * 2019-10-10 16:17:01,983 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 16:17:02,552 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 16:17:02,553 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local2074560084_0001 completed successfully
     * 2019-10-10 16:17:02,565 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=9909236
     * 		FILE: Number of bytes written=10797006
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=117032
     * 		Map output records=312084
     * 		Map output bytes=2789247
     * 		Map output materialized bytes=3413421
     * 		Input split bytes=223
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=11
     * 		Reduce shuffle bytes=3413421
     * 		Reduce input records=312084
     * 		Reduce output records=11
     * 		Spilled Records=624168
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=22
     * 		Total committed heap usage (bytes)=540016640
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=0
     * 	File Output Format Counters
     * 		Bytes Written=135
     *
     * Process finished with exit code 0
     */

}
