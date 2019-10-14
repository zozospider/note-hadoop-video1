package com.zozospider.hadoop.mapreduce.output.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 将输入的每 1 行内容按照首字母 (a-k, l-z) 输出到不同的文件中
 */
public class CustomDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/output/Custom/
     * total 8
     * -rw-r--r--  1 user  staff   109B 10 14 21:00 f1
     * ➜  input cat output/Custom/f1
     * abc do the best
     * why do you like
     * it's so cool
     * it's so cool
     * I don't think so
     * qq love good
     * zoo zoo zoo
     * book abc
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/output/Custom/
     * ls: /Users/user/other/tmp/MapReduce/output/output/Custom/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/output/Custom", "/Users/user/other/tmp/MapReduce/output/output/Custom"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 OutputFormat 为 CustomOutputFormat
        job.setOutputFormatClass(CustomOutputFormat.class);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CustomDriver.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/output/Custom/
     * total 16
     * -rw-r--r--  1 user  staff    78B 10 14 21:12 AToK.log
     * -rw-r--r--  1 user  staff    47B 10 14 21:12 LToZ.log
     * -rw-r--r--  1 user  staff     0B 10 14 21:12 _SUCCESS
     * ➜  output cat output/Custom/AToK.log
     * I don't think so
     * abc do the best
     * book abc
     * it's so cool
     * it's so cool
     * ➜  output cat output/Custom/LToZ.log
     * qq love good
     * why do you like
     * zoo zoo zoo
     * ➜  output
     */

    /**
     * 2019-10-14 21:12:14,654 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-14 21:12:14,941 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-14 21:12:14,943 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-14 21:12:15,599 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-14 21:12:15,604 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-14 21:12:15,622 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-14 21:12:15,671 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-14 21:12:15,785 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local729970595_0001
     * 2019-10-14 21:12:15,998 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-14 21:12:15,999 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local729970595_0001
     * 2019-10-14 21:12:16,000 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-14 21:12:16,005 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-14 21:12:16,007 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-14 21:12:16,058 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-14 21:12:16,059 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local729970595_0001_m_000000_0
     * 2019-10-14 21:12:16,092 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-14 21:12:16,100 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-14 21:12:16,101 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-14 21:12:16,106 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/output/Custom/f1:0+109
     * 2019-10-14 21:12:16,189 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-14 21:12:16,189 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-14 21:12:16,189 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-14 21:12:16,189 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-14 21:12:16,189 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-14 21:12:16,194 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-14 21:12:16,207 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-14 21:12:16,208 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-14 21:12:16,208 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-14 21:12:16,208 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 109; bufvoid = 104857600
     * 2019-10-14 21:12:16,208 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214368(104857472); length = 29/6553600
     * 2019-10-14 21:12:16,219 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-14 21:12:16,222 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local729970595_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-14 21:12:16,232 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-14 21:12:16,232 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local729970595_0001_m_000000_0' done.
     * 2019-10-14 21:12:16,232 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local729970595_0001_m_000000_0
     * 2019-10-14 21:12:16,232 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-14 21:12:16,234 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-14 21:12:16,236 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local729970595_0001_r_000000_0
     * 2019-10-14 21:12:16,243 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-14 21:12:16,244 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-14 21:12:16,244 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-14 21:12:16,246 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@5d7fd712
     * 2019-10-14 21:12:16,257 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-14 21:12:16,260 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local729970595_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-14 21:12:16,296 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local729970595_0001_m_000000_0 decomp: 127 len: 131 to MEMORY
     * 2019-10-14 21:12:16,310 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 127 bytes from map-output for attempt_local729970595_0001_m_000000_0
     * 2019-10-14 21:12:16,313 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 127, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->127
     * 2019-10-14 21:12:16,314 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-14 21:12:16,315 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-14 21:12:16,315 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-14 21:12:16,323 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-14 21:12:16,323 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 108 bytes
     * 2019-10-14 21:12:16,324 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 127 bytes to disk to satisfy reduce memory limit
     * 2019-10-14 21:12:16,325 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 131 bytes from disk
     * 2019-10-14 21:12:16,325 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-14 21:12:16,326 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-14 21:12:16,326 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 108 bytes
     * 2019-10-14 21:12:16,327 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-14 21:12:16,362 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-14 21:12:16,369 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local729970595_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-14 21:12:16,371 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-14 21:12:16,371 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local729970595_0001_r_000000_0' done.
     * 2019-10-14 21:12:16,371 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local729970595_0001_r_000000_0
     * 2019-10-14 21:12:16,371 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-14 21:12:17,005 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local729970595_0001 running in uber mode : false
     * 2019-10-14 21:12:17,007 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-14 21:12:17,008 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local729970595_0001 completed successfully
     * 2019-10-14 21:12:17,019 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=868
     * 		FILE: Number of bytes written=553906
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=8
     * 		Map output records=8
     * 		Map output bytes=109
     * 		Map output materialized bytes=131
     * 		Input split bytes=124
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=7
     * 		Reduce shuffle bytes=131
     * 		Reduce input records=8
     * 		Reduce output records=8
     * 		Spilled Records=16
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=10
     * 		Total committed heap usage (bytes)=502792192
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=109
     * 	File Output Format Counters
     * 		Bytes Written=149
     *
     * Process finished with exit code 0
     */

}
