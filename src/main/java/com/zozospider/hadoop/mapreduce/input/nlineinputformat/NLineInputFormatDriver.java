package com.zozospider.hadoop.mapreduce.input.nlineinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动
 */
public class NLineInputFormatDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/nlineinputformat
     * total 8
     * -rw-r--r--  1 user  staff    59B 10 10 13:38 f1
     * ➜  input cat nlineinputformat/f1
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * ➜  input
     * <p>
     * ➜  input ll /Users/user/other/tmp/mapReduce/output/nlineinputformat
     * ls: /Users/user/other/tmp/mapReduce/output/nlineinputformat: No such file or directory
     * ➜  input
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/nlineinputformat", "/Users/user/other/tmp/mapReduce/output/nlineinputformat"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: NLineInputFormat
        job.setInputFormatClass(NLineInputFormat.class);
        // 设置 NLineInputFormat 每 3 行划分 1 个切片
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(NLineInputFormatDriver.class);
        job.setMapperClass(NLineInputFormatMapper.class);
        job.setReducerClass(NLineInputFormatReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/nlineinputformat
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 13:41 _SUCCESS
     * -rw-r--r--  1 user  staff    65B 10 10 13:41 part-r-00000
     * ➜  output cat nlineinputformat/part-r-00000
     * abc	2
     * awesome	1
     * book	1
     * google	1
     * love	2
     * please	1
     * qq	2
     * see	1
     * who	1
     * ➜  output
     */

    /**
     * 2019-10-10 13:41:00,719 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 13:41:01,043 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 13:41:01,046 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 13:41:02,023 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 13:41:02,028 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 13:41:02,044 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-10 13:41:02,104 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:2
     * 2019-10-10 13:41:02,377 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1747101776_0001
     * 2019-10-10 13:41:02,683 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 13:41:02,684 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1747101776_0001
     * 2019-10-10 13:41:02,687 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 13:41:02,699 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 13:41:02,702 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 13:41:02,774 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 13:41:02,774 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1747101776_0001_m_000000_0
     * 2019-10-10 13:41:02,846 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 13:41:02,856 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 13:41:02,856 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 13:41:02,861 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/mapReduce/input/nlineinputformat/f1:0+32
     * 2019-10-10 13:41:03,038 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 13:41:03,038 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 13:41:03,038 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 13:41:03,038 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 13:41:03,038 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 13:41:03,042 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 13:41:03,066 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 13:41:03,066 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 13:41:03,066 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 13:41:03,066 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 61; bufvoid = 104857600
     * 2019-10-10 13:41:03,067 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214372(104857488); length = 25/6553600
     * 2019-10-10 13:41:03,091 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 13:41:03,098 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1747101776_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 13:41:03,109 INFO [org.apache.hadoop.mapred.LocalJobRunner] - file:/Users/user/other/tmp/mapReduce/input/nlineinputformat/f1:0+32
     * 2019-10-10 13:41:03,109 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1747101776_0001_m_000000_0' done.
     * 2019-10-10 13:41:03,109 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1747101776_0001_m_000000_0
     * 2019-10-10 13:41:03,109 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1747101776_0001_m_000001_0
     * 2019-10-10 13:41:03,114 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 13:41:03,114 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 13:41:03,114 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 13:41:03,116 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/mapReduce/input/nlineinputformat/f1:32+26
     * 2019-10-10 13:41:03,161 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 13:41:03,161 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 13:41:03,161 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 13:41:03,161 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 13:41:03,161 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 13:41:03,162 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 13:41:03,173 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 13:41:03,173 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 13:41:03,173 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 13:41:03,173 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 46; bufvoid = 104857600
     * 2019-10-10 13:41:03,173 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214380(104857520); length = 17/6553600
     * 2019-10-10 13:41:03,175 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 13:41:03,182 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1747101776_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 13:41:03,184 INFO [org.apache.hadoop.mapred.LocalJobRunner] - file:/Users/user/other/tmp/mapReduce/input/nlineinputformat/f1:32+26
     * 2019-10-10 13:41:03,184 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1747101776_0001_m_000001_0' done.
     * 2019-10-10 13:41:03,184 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1747101776_0001_m_000001_0
     * 2019-10-10 13:41:03,185 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 13:41:03,190 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 13:41:03,190 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1747101776_0001_r_000000_0
     * 2019-10-10 13:41:03,201 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 13:41:03,202 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 13:41:03,202 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 13:41:03,210 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@1bf7dcc9
     * 2019-10-10 13:41:03,244 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 13:41:03,247 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1747101776_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 13:41:03,301 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1747101776_0001_m_000000_0 decomp: 77 len: 81 to MEMORY
     * 2019-10-10 13:41:03,304 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 77 bytes from map-output for attempt_local1747101776_0001_m_000000_0
     * 2019-10-10 13:41:03,306 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 77, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->77
     * 2019-10-10 13:41:03,310 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1747101776_0001_m_000001_0 decomp: 58 len: 62 to MEMORY
     * 2019-10-10 13:41:03,311 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 58 bytes from map-output for attempt_local1747101776_0001_m_000001_0
     * 2019-10-10 13:41:03,311 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 58, inMemoryMapOutputs.size() -> 2, commitMemory -> 77, usedMemory ->135
     * 2019-10-10 13:41:03,315 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 13:41:03,323 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-10 13:41:03,325 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 2 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 13:41:03,345 INFO [org.apache.hadoop.mapred.Merger] - Merging 2 sorted segments
     * 2019-10-10 13:41:03,346 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 2 segments left of total size: 120 bytes
     * 2019-10-10 13:41:03,347 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 2 segments, 135 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 13:41:03,348 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 137 bytes from disk
     * 2019-10-10 13:41:03,348 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 13:41:03,348 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 13:41:03,349 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 127 bytes
     * 2019-10-10 13:41:03,350 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-10 13:41:03,378 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 13:41:03,389 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1747101776_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 13:41:03,391 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-10 13:41:03,391 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1747101776_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 13:41:03,392 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1747101776_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/nlineinputformat/_temporary/0/task_local1747101776_0001_r_000000
     * 2019-10-10 13:41:03,393 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 13:41:03,393 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1747101776_0001_r_000000_0' done.
     * 2019-10-10 13:41:03,393 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1747101776_0001_r_000000_0
     * 2019-10-10 13:41:03,393 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 13:41:03,704 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1747101776_0001 running in uber mode : false
     * 2019-10-10 13:41:03,729 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 13:41:03,731 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1747101776_0001 completed successfully
     * 2019-10-10 13:41:03,746 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=2204
     * 		FILE: Number of bytes written=834621
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=5
     * 		Map output records=12
     * 		Map output bytes=107
     * 		Map output materialized bytes=143
     * 		Input split bytes=254
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=9
     * 		Reduce shuffle bytes=143
     * 		Reduce input records=12
     * 		Reduce output records=9
     * 		Spilled Records=24
     * 		Shuffled Maps =2
     * 		Failed Shuffles=0
     * 		Merged Map outputs=2
     * 		GC time elapsed (ms)=12
     * 		Total committed heap usage (bytes)=913833984
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=86
     * 	File Output Format Counters
     * 		Bytes Written=77
     *
     * Process finished with exit code 0
     */

}
