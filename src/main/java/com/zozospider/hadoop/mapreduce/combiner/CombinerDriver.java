package com.zozospider.hadoop.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 统计单次出现次数
 */
public class CombinerDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/Combiner/
     * total 8
     * -rw-r--r--  1 user  staff    79B 10 11 20:57 f1
     * ➜  input cat Combiner/f1
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/Combiner/
     * ls: /Users/user/other/tmp/MapReduce/output/Combiner/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/Combiner", "/Users/user/other/tmp/MapReduce/output/Combiner"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Combiner 为 CombinerCombiner
        job.setCombinerClass(CombinerCombiner.class);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombinerDriver.class);
        job.setMapperClass(CombinerMapper.class);
        job.setReducerClass(CombinerReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/Combiner/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 11 20:58 _SUCCESS
     * -rw-r--r--  1 user  staff    79B 10 11 20:58 part-r-00000
     * ➜  output cat Combiner/part-r-00000
     * abc	2
     * awesome	1
     * book	2
     * enough	1
     * google	1
     * love	3
     * me	1
     * please	1
     * qq	2
     * see	1
     * who	1
     * ➜  output
     */

    /**
     * 2019-10-11 20:58:40,390 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-11 20:58:40,704 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-11 20:58:40,706 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-11 20:58:41,630 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-11 20:58:41,634 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-11 20:58:41,671 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-11 20:58:41,768 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-11 20:58:42,050 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local408288425_0001
     * 2019-10-11 20:58:42,377 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-11 20:58:42,379 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local408288425_0001
     * 2019-10-11 20:58:42,380 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-11 20:58:42,388 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 20:58:42,390 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-11 20:58:42,462 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-11 20:58:42,462 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local408288425_0001_m_000000_0
     * 2019-10-11 20:58:42,526 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 20:58:42,556 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-11 20:58:42,557 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-11 20:58:42,563 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/Combiner/f1:0+79
     * 2019-10-11 20:58:42,686 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-11 20:58:42,686 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-11 20:58:42,686 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-11 20:58:42,686 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-11 20:58:42,686 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-11 20:58:42,692 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-11 20:58:42,712 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-11 20:58:42,712 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-11 20:58:42,712 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-11 20:58:42,712 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 143; bufvoid = 104857600
     * 2019-10-11 20:58:42,712 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214336(104857344); length = 61/6553600
     * 2019-10-11 20:58:42,771 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-11 20:58:42,777 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local408288425_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-11 20:58:42,788 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-11 20:58:42,789 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local408288425_0001_m_000000_0' done.
     * 2019-10-11 20:58:42,792 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local408288425_0001_m_000000_0
     * 2019-10-11 20:58:42,792 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-11 20:58:42,796 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-11 20:58:42,796 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local408288425_0001_r_000000_0
     * 2019-10-11 20:58:42,803 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 20:58:42,804 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-11 20:58:42,804 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-11 20:58:42,809 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@5ba6d82c
     * 2019-10-11 20:58:42,841 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-11 20:58:42,844 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local408288425_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-11 20:58:42,888 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local408288425_0001_m_000000_0 decomp: 125 len: 129 to MEMORY
     * 2019-10-11 20:58:42,905 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 125 bytes from map-output for attempt_local408288425_0001_m_000000_0
     * 2019-10-11 20:58:42,908 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 125, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->125
     * 2019-10-11 20:58:42,910 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-11 20:58:42,912 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 20:58:42,912 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-11 20:58:42,919 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-11 20:58:42,919 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 119 bytes
     * 2019-10-11 20:58:42,921 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 125 bytes to disk to satisfy reduce memory limit
     * 2019-10-11 20:58:42,921 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 129 bytes from disk
     * 2019-10-11 20:58:42,921 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-11 20:58:42,922 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-11 20:58:42,922 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 119 bytes
     * 2019-10-11 20:58:42,923 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 20:58:42,949 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-11 20:58:42,952 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local408288425_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-11 20:58:42,954 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 20:58:42,954 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local408288425_0001_r_000000_0 is allowed to commit now
     * 2019-10-11 20:58:42,966 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local408288425_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/Combiner/_temporary/0/task_local408288425_0001_r_000000
     * 2019-10-11 20:58:42,967 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-11 20:58:42,967 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local408288425_0001_r_000000_0' done.
     * 2019-10-11 20:58:42,967 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local408288425_0001_r_000000_0
     * 2019-10-11 20:58:42,967 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-11 20:58:43,387 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local408288425_0001 running in uber mode : false
     * 2019-10-11 20:58:43,444 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-11 20:58:43,445 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local408288425_0001 completed successfully
     * 2019-10-11 20:58:43,454 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=794
     * 		FILE: Number of bytes written=553712
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=6
     * 		Map output records=16
     * 		Map output bytes=143
     * 		Map output materialized bytes=129
     * 		Input split bytes=119
     * 		Combine input records=16
     * 		Combine output records=11
     * 		Reduce input groups=11
     * 		Reduce shuffle bytes=129
     * 		Reduce input records=11
     * 		Reduce output records=11
     * 		Spilled Records=22
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=10
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
     * 		Bytes Written=91
     *
     * Process finished with exit code 0
     */

}
