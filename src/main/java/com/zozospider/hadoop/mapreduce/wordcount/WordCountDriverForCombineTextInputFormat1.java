package com.zozospider.hadoop.mapreduce.wordcount;

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
public class WordCountDriverForCombineTextInputFormat1 {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/wordCount/
     * total 48488
     * -rw-r--r--  1 user  staff   1.5M 10  9 20:01 f1
     * -rw-r--r--  1 user  staff   5.1M 10  9 20:02 f2
     * -rw-r--r--  1 user  staff   6.8M 10  9 20:04 f3
     * -rw-r--r--  1 user  staff    10M 10  9 20:04 f4
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/wordCount/
     * ls: /Users/user/other/tmp/mapReduce/output/wordCount: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/wordCount", "/Users/user/other/tmp/mapReduce/output/wordCount"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(WordCountDriverForCombineTextInputFormat1.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/wordCount/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10  9 20:22 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10  9 20:22 part-r-00000
     * ➜  output cat wordCount/part-r-00000
     * 	394
     * abc	628188
     * awesome	314013
     * book	628094
     * enough	314081
     * google	314015
     * love	942191
     * me	314081
     * please	314015
     * qq	628422
     * see	314013
     * who	314016
     * ➜  output
     */

    /**
     * 2019-10-09 20:22:40,218 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-09 20:22:40,543 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-09 20:22:40,544 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-09 20:22:41,442 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-09 20:22:41,448 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-09 20:22:41,498 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 4
     * 2019-10-09 20:22:41,553 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
     * 2019-10-09 20:22:41,593 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     * 2019-10-09 20:22:41,714 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1057722139_0001
     * 2019-10-09 20:22:41,936 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-09 20:22:41,941 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-09 20:22:41,942 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:22:41,945 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1057722139_0001
     * 2019-10-09 20:22:41,947 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-09 20:22:42,000 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-09 20:22:42,001 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1057722139_0001_m_000000_0
     * 2019-10-09 20:22:42,032 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:22:42,042 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:22:42,042 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:22:42,047 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f3:3580272+3580273,/Users/user/other/tmp/mapReduce/input/wordCount/f4:0+4194304
     * 2019-10-09 20:22:42,117 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:22:42,117 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:22:42,117 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:22:42,117 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:22:42,117 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:22:42,119 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:22:42,952 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1057722139_0001 running in uber mode : false
     * 2019-10-09 20:22:42,954 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-09 20:22:43,059 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:22:43,059 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:22:43,059 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:22:43,059 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 14073960; bufvoid = 104857600
     * 2019-10-09 20:22:43,059 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 19915020(79660080); length = 6299377/6553600
     * 2019-10-09 20:22:43,894 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:22:43,911 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1057722139_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-09 20:22:43,919 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:22:43,919 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1057722139_0001_m_000000_0' done.
     * 2019-10-09 20:22:43,919 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1057722139_0001_m_000000_0
     * 2019-10-09 20:22:43,919 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1057722139_0001_m_000001_0
     * 2019-10-09 20:22:43,920 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:22:43,921 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:22:43,921 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:22:43,922 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f4:4194304+3272748,/Users/user/other/tmp/mapReduce/input/wordCount/f4:7467052+3272748
     * 2019-10-09 20:22:43,943 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:22:43,943 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:22:43,943 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:22:43,943 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:22:43,943 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:22:43,944 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:22:43,957 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-09 20:22:44,594 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:22:44,594 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:22:44,594 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:22:44,594 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11848811; bufvoid = 104857600
     * 2019-10-09 20:22:44,594 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 20911076(83644304); length = 5303321/6553600
     * 2019-10-09 20:22:44,957 INFO [org.apache.hadoop.mapreduce.Job] -  map 25% reduce 0%
     * 2019-10-09 20:22:45,199 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:22:45,209 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1057722139_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-09 20:22:45,211 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:22:45,211 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1057722139_0001_m_000001_0' done.
     * 2019-10-09 20:22:45,211 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1057722139_0001_m_000001_0
     * 2019-10-09 20:22:45,211 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1057722139_0001_m_000002_0
     * 2019-10-09 20:22:45,212 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:22:45,213 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:22:45,213 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:22:45,214 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f2:2684950+2684950,/Users/user/other/tmp/mapReduce/input/wordCount/f3:0+3580272
     * 2019-10-09 20:22:45,242 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:22:45,243 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:22:45,243 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:22:45,243 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:22:45,243 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:22:45,243 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:22:45,882 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:22:45,882 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:22:45,882 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:22:45,882 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-09 20:22:45,882 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-09 20:22:45,958 INFO [org.apache.hadoop.mapreduce.Job] -  map 50% reduce 0%
     * 2019-10-09 20:22:46,424 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:22:46,434 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1057722139_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-09 20:22:46,436 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:22:46,436 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1057722139_0001_m_000002_0' done.
     * 2019-10-09 20:22:46,436 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1057722139_0001_m_000002_0
     * 2019-10-09 20:22:46,436 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1057722139_0001_m_000003_0
     * 2019-10-09 20:22:46,437 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:22:46,437 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:22:46,437 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:22:46,439 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f1:0+1540832,/Users/user/other/tmp/mapReduce/input/wordCount/f2:0+2684950
     * 2019-10-09 20:22:46,465 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:22:46,465 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:22:46,465 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:22:46,465 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:22:46,465 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:22:46,466 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:22:46,784 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:22:46,784 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:22:46,784 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:22:46,784 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649219; bufvoid = 104857600
     * 2019-10-09 20:22:46,784 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790968(91163872); length = 3423429/6553600
     * 2019-10-09 20:22:46,959 INFO [org.apache.hadoop.mapreduce.Job] -  map 75% reduce 0%
     * 2019-10-09 20:22:47,176 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:22:47,187 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1057722139_0001_m_000003_0 is done. And is in the process of committing
     * 2019-10-09 20:22:47,189 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:22:47,189 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1057722139_0001_m_000003_0' done.
     * 2019-10-09 20:22:47,189 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1057722139_0001_m_000003_0
     * 2019-10-09 20:22:47,190 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-09 20:22:47,193 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-09 20:22:47,194 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1057722139_0001_r_000000_0
     * 2019-10-09 20:22:47,202 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:22:47,202 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:22:47,202 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:22:47,206 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@410b568d
     * 2019-10-09 20:22:47,220 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-09 20:22:47,223 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1057722139_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-09 20:22:47,280 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1057722139_0001_m_000000_0 decomp: 17223652 len: 17223656 to MEMORY
     * 2019-10-09 20:22:47,308 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 17223652 bytes from map-output for attempt_local1057722139_0001_m_000000_0
     * 2019-10-09 20:22:47,309 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 17223652, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->17223652
     * 2019-10-09 20:22:47,317 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1057722139_0001_m_000003_0 decomp: 9360937 len: 9360941 to MEMORY
     * 2019-10-09 20:22:47,329 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9360937 bytes from map-output for attempt_local1057722139_0001_m_000003_0
     * 2019-10-09 20:22:47,330 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9360937, inMemoryMapOutputs.size() -> 2, commitMemory -> 17223652, usedMemory ->26584589
     * 2019-10-09 20:22:47,339 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1057722139_0001_m_000002_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-09 20:22:47,356 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local1057722139_0001_m_000002_0
     * 2019-10-09 20:22:47,356 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 3, commitMemory -> 26584589, usedMemory ->40463748
     * 2019-10-09 20:22:47,366 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1057722139_0001_m_000001_0 decomp: 14500475 len: 14500479 to MEMORY
     * 2019-10-09 20:22:47,388 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 14500475 bytes from map-output for attempt_local1057722139_0001_m_000001_0
     * 2019-10-09 20:22:47,389 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 14500475, inMemoryMapOutputs.size() -> 4, commitMemory -> 40463748, usedMemory ->54964223
     * 2019-10-09 20:22:47,389 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-09 20:22:47,390 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-09 20:22:47,459 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 4 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-09 20:22:47,466 INFO [org.apache.hadoop.mapred.Merger] - Merging 4 sorted segments
     * 2019-10-09 20:22:47,466 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 4 segments left of total size: 54964208 bytes
     * 2019-10-09 20:22:47,960 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-09 20:22:49,055 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 4 segments, 54964223 bytes to disk to satisfy reduce memory limit
     * 2019-10-09 20:22:49,055 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 54964221 bytes from disk
     * 2019-10-09 20:22:49,056 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-09 20:22:49,056 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-09 20:22:49,056 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 54964214 bytes
     * 2019-10-09 20:22:49,057 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-09 20:22:49,067 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-09 20:22:50,642 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1057722139_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-09 20:22:50,643 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-09 20:22:50,643 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1057722139_0001_r_000000_0 is allowed to commit now
     * 2019-10-09 20:22:50,644 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1057722139_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/wordCount/_temporary/0/task_local1057722139_0001_r_000000
     * 2019-10-09 20:22:50,644 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-09 20:22:50,645 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1057722139_0001_r_000000_0' done.
     * 2019-10-09 20:22:50,645 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1057722139_0001_r_000000_0
     * 2019-10-09 20:22:50,645 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-09 20:22:50,967 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-09 20:22:50,967 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1057722139_0001 completed successfully
     * 2019-10-09 20:22:50,981 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=202331492
     * 		FILE: Number of bytes written=260839087
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1885019
     * 		Map output records=5025523
     * 		Map output bytes=44913169
     * 		Map output materialized bytes=54964239
     * 		Input split bytes=876
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=12
     * 		Reduce shuffle bytes=54964239
     * 		Reduce input records=5025523
     * 		Reduce output records=12
     * 		Spilled Records=10051046
     * 		Shuffled Maps =4
     * 		Failed Shuffles=0
     * 		Merged Map outputs=4
     * 		GC time elapsed (ms)=386
     * 		Total committed heap usage (bytes)=3228041216
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
