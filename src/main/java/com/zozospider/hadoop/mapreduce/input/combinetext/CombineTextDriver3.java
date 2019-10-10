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
public class CombineTextDriver3 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/CombineText3/
     * total 27512
     * -rw-r--r--  1 user  staff    79B 10 10 13:53 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:53 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:53 c
     * -rw-r--r--  1 user  staff   6.8M 10 10 13:53 d
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText3/
     * ls: /Users/user/other/tmp/MapReduce/output/CombineText3/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/CombineText3", "/Users/user/other/tmp/MapReduce/output/CombineText3"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextDriver3.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText3/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 16:18 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10 10 16:18 part-r-00000
     * ➜  output cat CombineText3/part-r-00000
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
     * 2019-10-10 16:18:21,186 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 16:18:21,456 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 16:18:21,457 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 16:18:21,860 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 16:18:21,864 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 16:18:21,897 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 4
     * 2019-10-10 16:18:21,916 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 3580273
     * 2019-10-10 16:18:21,954 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 16:18:22,057 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local492795048_0001
     * 2019-10-10 16:18:22,266 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 16:18:22,268 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 16:18:22,270 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local492795048_0001
     * 2019-10-10 16:18:22,274 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:18:22,276 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 16:18:22,326 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 16:18:22,327 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local492795048_0001_m_000000_0
     * 2019-10-10 16:18:22,356 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:18:22,362 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:18:22,363 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:18:22,368 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText3/c:2684950+2684950,/Users/user/other/tmp/MapReduce/input/CombineText3/d:0+3580272
     * 2019-10-10 16:18:22,451 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:18:22,451 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:18:22,452 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:18:22,452 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:18:22,452 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:18:22,454 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:18:23,258 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:18:23,258 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:18:23,258 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:18:23,258 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-10 16:18:23,258 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-10 16:18:23,276 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local492795048_0001 running in uber mode : false
     * 2019-10-10 16:18:23,278 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 16:18:24,060 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:18:24,076 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local492795048_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:18:24,086 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:18:24,086 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local492795048_0001_m_000000_0' done.
     * 2019-10-10 16:18:24,086 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local492795048_0001_m_000000_0
     * 2019-10-10 16:18:24,086 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local492795048_0001_m_000001_0
     * 2019-10-10 16:18:24,087 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:18:24,088 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:18:24,088 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:18:24,089 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText3/a:0+79,/Users/user/other/tmp/MapReduce/input/CombineText3/b:0+1540832,/Users/user/other/tmp/MapReduce/input/CombineText3/c:0+2684950
     * 2019-10-10 16:18:24,136 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:18:24,136 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:18:24,136 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:18:24,136 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:18:24,136 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:18:24,137 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:18:24,284 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 16:18:24,551 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:18:24,551 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:18:24,552 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:18:24,552 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649362; bufvoid = 104857600
     * 2019-10-10 16:18:24,552 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790904(91163616); length = 3423493/6553600
     * 2019-10-10 16:18:24,938 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:18:24,948 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local492795048_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 16:18:24,950 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:18:24,950 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local492795048_0001_m_000001_0' done.
     * 2019-10-10 16:18:24,950 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local492795048_0001_m_000001_0
     * 2019-10-10 16:18:24,950 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local492795048_0001_m_000002_0
     * 2019-10-10 16:18:24,951 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:18:24,952 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:18:24,952 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:18:24,953 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText3/d:3580272+3580273
     * 2019-10-10 16:18:24,980 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:18:24,980 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:18:24,980 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:18:24,980 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:18:24,980 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:18:24,981 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:18:25,527 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:18:25,527 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:18:25,527 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:18:25,527 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 6481715; bufvoid = 104857600
     * 2019-10-10 16:18:25,527 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 23312952(93251808); length = 2901445/6553600
     * 2019-10-10 16:18:25,839 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:18:25,845 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local492795048_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 16:18:25,846 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:18:25,846 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local492795048_0001_m_000002_0' done.
     * 2019-10-10 16:18:25,846 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local492795048_0001_m_000002_0
     * 2019-10-10 16:18:25,846 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 16:18:25,848 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 16:18:25,848 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local492795048_0001_r_000000_0
     * 2019-10-10 16:18:25,854 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:18:25,855 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:18:25,855 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:18:25,857 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@4a100a4
     * 2019-10-10 16:18:25,868 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 16:18:25,870 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local492795048_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 16:18:25,906 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local492795048_0001_m_000002_0 decomp: 7932441 len: 7932445 to MEMORY
     * 2019-10-10 16:18:25,920 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 7932441 bytes from map-output for attempt_local492795048_0001_m_000002_0
     * 2019-10-10 16:18:25,922 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 7932441, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->7932441
     * 2019-10-10 16:18:25,927 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local492795048_0001_m_000000_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-10 16:18:25,947 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local492795048_0001_m_000000_0
     * 2019-10-10 16:18:25,947 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 2, commitMemory -> 7932441, usedMemory ->21811600
     * 2019-10-10 16:18:25,950 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local492795048_0001_m_000001_0 decomp: 9361112 len: 9361116 to MEMORY
     * 2019-10-10 16:18:25,961 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9361112 bytes from map-output for attempt_local492795048_0001_m_000001_0
     * 2019-10-10 16:18:25,961 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9361112, inMemoryMapOutputs.size() -> 3, commitMemory -> 21811600, usedMemory ->31172712
     * 2019-10-10 16:18:25,962 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 16:18:25,963 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 16:18:25,963 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 16:18:25,968 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 16:18:25,969 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 31172700 bytes
     * 2019-10-10 16:18:26,886 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 31172712 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 16:18:26,886 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 31172712 bytes from disk
     * 2019-10-10 16:18:26,886 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 16:18:26,887 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:18:26,887 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 31172705 bytes
     * 2019-10-10 16:18:26,888 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 16:18:26,899 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 16:18:28,807 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local492795048_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:18:28,809 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 16:18:28,809 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local492795048_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 16:18:28,810 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local492795048_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/CombineText3/_temporary/0/task_local492795048_0001_r_000000
     * 2019-10-10 16:18:28,810 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 16:18:28,811 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local492795048_0001_r_000000_0' done.
     * 2019-10-10 16:18:28,811 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local492795048_0001_r_000000_0
     * 2019-10-10 16:18:28,811 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 16:18:29,412 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 16:18:29,413 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local492795048_0001 completed successfully
     * 2019-10-10 16:18:29,426 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=107299667
     * 		FILE: Number of bytes written=131747069
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1069133
     * 		Map output records=2850225
     * 		Map output bytes=25472256
     * 		Map output materialized bytes=31172724
     * 		Input split bytes=669
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
     * 		GC time elapsed (ms)=312
     * 		Total committed heap usage (bytes)=2078277632
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
