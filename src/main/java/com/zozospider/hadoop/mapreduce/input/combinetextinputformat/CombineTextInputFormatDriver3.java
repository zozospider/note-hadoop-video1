package com.zozospider.hadoop.mapreduce.input.combinetextinputformat;

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
 */
public class CombineTextInputFormatDriver3 {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/combinetextinputformat3/
     * total 27512
     * -rw-r--r--  1 user  staff    79B 10 10 13:53 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:53 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:53 c
     * -rw-r--r--  1 user  staff   6.8M 10 10 13:53 d
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat3/
     * ls: /Users/user/other/tmp/mapReduce/output/combinetextinputformat3/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/combinetextinputformat3", "/Users/user/other/tmp/mapReduce/output/combinetextinputformat3"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextInputFormatDriver3.class);
        job.setMapperClass(CombineTextInputFormatMapper.class);
        job.setReducerClass(CombineTextInputFormatReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat3/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 14:12 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10 10 14:12 part-r-00000
     * ➜  output cat combinetextinputformat3/part-r-00000
     * 	264
     * abc	356274
     * awesome	178084
     * book	356214
     * enough	178130
     * google	178086
     * love	534354
     * me	178130
     * please	178086
     * qq	356432
     * see	178084
     * who	178087
     * ➜  output
     */

    /**
     * 2019-10-10 14:12:15,167 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 14:12:15,435 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 14:12:15,436 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 14:12:15,973 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 14:12:15,978 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 14:12:16,012 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 4
     * 2019-10-10 14:12:16,032 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 3580273
     * 2019-10-10 14:12:16,069 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 14:12:16,173 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1464736986_0001
     * 2019-10-10 14:12:16,374 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 14:12:16,375 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1464736986_0001
     * 2019-10-10 14:12:16,375 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 14:12:16,381 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:12:16,383 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 14:12:16,428 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 14:12:16,428 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1464736986_0001_m_000000_0
     * 2019-10-10 14:12:16,459 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:12:16,465 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:12:16,466 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:12:16,472 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat3/c:2684950+2684950,/Users/user/other/tmp/mapReduce/input/combinetextinputformat3/d:0+3580272
     * 2019-10-10 14:12:16,546 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:12:16,546 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:12:16,546 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:12:16,546 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:12:16,546 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:12:16,548 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:12:17,363 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:12:17,363 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:12:17,363 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:12:17,363 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-10 14:12:17,363 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-10 14:12:17,381 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1464736986_0001 running in uber mode : false
     * 2019-10-10 14:12:17,383 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 14:12:18,174 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:12:18,190 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1464736986_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:12:18,198 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:12:18,198 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1464736986_0001_m_000000_0' done.
     * 2019-10-10 14:12:18,198 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1464736986_0001_m_000000_0
     * 2019-10-10 14:12:18,199 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1464736986_0001_m_000001_0
     * 2019-10-10 14:12:18,200 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:12:18,200 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:12:18,200 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:12:18,201 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat3/a:0+79,/Users/user/other/tmp/mapReduce/input/combinetextinputformat3/b:0+1540832,/Users/user/other/tmp/mapReduce/input/combinetextinputformat3/c:0+2684950
     * 2019-10-10 14:12:18,247 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:12:18,247 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:12:18,247 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:12:18,247 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:12:18,247 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:12:18,248 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:12:18,386 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 14:12:18,595 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:12:18,595 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:12:18,595 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:12:18,595 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649362; bufvoid = 104857600
     * 2019-10-10 14:12:18,595 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790904(91163616); length = 3423493/6553600
     * 2019-10-10 14:12:18,990 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:12:19,005 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1464736986_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 14:12:19,007 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:12:19,007 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1464736986_0001_m_000001_0' done.
     * 2019-10-10 14:12:19,007 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1464736986_0001_m_000001_0
     * 2019-10-10 14:12:19,007 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1464736986_0001_m_000002_0
     * 2019-10-10 14:12:19,008 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:12:19,009 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:12:19,009 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:12:19,011 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat3/d:3580272+3580273
     * 2019-10-10 14:12:19,036 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:12:19,036 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:12:19,036 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:12:19,036 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:12:19,036 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:12:19,037 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:12:19,439 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:12:19,440 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:12:19,440 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:12:19,440 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 6481715; bufvoid = 104857600
     * 2019-10-10 14:12:19,440 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 23312952(93251808); length = 2901445/6553600
     * 2019-10-10 14:12:19,752 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:12:19,759 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1464736986_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 14:12:19,761 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:12:19,762 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1464736986_0001_m_000002_0' done.
     * 2019-10-10 14:12:19,762 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1464736986_0001_m_000002_0
     * 2019-10-10 14:12:19,762 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 14:12:19,765 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 14:12:19,765 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1464736986_0001_r_000000_0
     * 2019-10-10 14:12:19,773 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:12:19,773 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:12:19,773 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:12:19,776 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6875d41d
     * 2019-10-10 14:12:19,786 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 14:12:19,789 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1464736986_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 14:12:19,827 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1464736986_0001_m_000002_0 decomp: 7932441 len: 7932445 to MEMORY
     * 2019-10-10 14:12:19,839 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 7932441 bytes from map-output for attempt_local1464736986_0001_m_000002_0
     * 2019-10-10 14:12:19,841 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 7932441, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->7932441
     * 2019-10-10 14:12:19,845 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1464736986_0001_m_000001_0 decomp: 9361112 len: 9361116 to MEMORY
     * 2019-10-10 14:12:19,858 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9361112 bytes from map-output for attempt_local1464736986_0001_m_000001_0
     * 2019-10-10 14:12:19,858 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9361112, inMemoryMapOutputs.size() -> 2, commitMemory -> 7932441, usedMemory ->17293553
     * 2019-10-10 14:12:19,862 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1464736986_0001_m_000000_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-10 14:12:19,882 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local1464736986_0001_m_000000_0
     * 2019-10-10 14:12:19,882 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 3, commitMemory -> 17293553, usedMemory ->31172712
     * 2019-10-10 14:12:19,883 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 14:12:19,884 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 14:12:19,884 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 14:12:19,890 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 14:12:19,890 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 31172700 bytes
     * 2019-10-10 14:12:20,834 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 31172712 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 14:12:20,834 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 31172712 bytes from disk
     * 2019-10-10 14:12:20,835 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 14:12:20,835 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 14:12:20,835 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 31172705 bytes
     * 2019-10-10 14:12:20,835 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 14:12:20,846 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 14:12:22,666 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1464736986_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:12:22,667 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 14:12:22,668 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1464736986_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 14:12:22,669 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1464736986_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/combinetextinputformat3/_temporary/0/task_local1464736986_0001_r_000000
     * 2019-10-10 14:12:22,670 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 14:12:22,670 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1464736986_0001_r_000000_0' done.
     * 2019-10-10 14:12:22,670 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1464736986_0001_r_000000_0
     * 2019-10-10 14:12:22,670 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 14:12:23,404 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 14:12:23,404 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1464736986_0001 completed successfully
     * 2019-10-10 14:12:23,417 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=107300261
     * 		FILE: Number of bytes written=131753861
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1069133
     * 		Map output records=2850225
     * 		Map output bytes=25472256
     * 		Map output materialized bytes=31172724
     * 		Input split bytes=735
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=12
     * 		Reduce shuffle bytes=31172724
     * 		Reduce input records=2850225
     * 		Reduce output records=12
     * 		Spilled Records=5700450
     * 		Shuffled Maps =3
     * 		Failed Shuffles=0
     * 		Merged Map outputs=3
     * 		GC time elapsed (ms)=197
     * 		Total committed heap usage (bytes)=2080374784
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
     * 		Bytes Written=151
     *
     * Process finished with exit code 0
     */

}
