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
public class WordCountDriverForCombineTextInputFormat2 {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/wordCount/
     * total 48496
     * -rw-r--r--  1 user  staff    79B 10  9 19:55 f0
     * -rw-r--r--  1 user  staff   1.5M 10  9 20:01 f1
     * -rw-r--r--  1 user  staff   5.1M 10  9 20:02 f2
     * -rw-r--r--  1 user  staff   6.8M 10  9 20:04 f3
     * -rw-r--r--  1 user  staff    10M 10  9 20:04 f4
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/wordCount/
     * ls: /Users/user/other/tmp/mapReduce/output/wordCount/: No such file or directory
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
        job.setJarByClass(WordCountDriverForCombineTextInputFormat2.class);
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
     * -rw-r--r--  1 user  staff     0B 10  9 20:26 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10  9 20:26 part-r-00000
     * ➜  output cat wordCount/part-r-00000
     * 	394
     * abc	628190
     * awesome	314014
     * book	628096
     * enough	314082
     * google	314016
     * love	942194
     * me	314082
     * please	314016
     * qq	628424
     * see	314014
     * who	314017
     * ➜  output
     */

    /**
     * 2019-10-09 20:26:44,723 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-09 20:26:45,007 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-09 20:26:45,011 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-09 20:26:45,638 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-09 20:26:45,642 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-09 20:26:45,690 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 5
     * 2019-10-09 20:26:45,710 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
     * 2019-10-09 20:26:45,751 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     * 2019-10-09 20:26:45,866 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local520479132_0001
     * 2019-10-09 20:26:46,080 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-09 20:26:46,084 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-09 20:26:46,086 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:26:46,088 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local520479132_0001
     * 2019-10-09 20:26:46,089 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-09 20:26:46,139 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-09 20:26:46,139 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local520479132_0001_m_000000_0
     * 2019-10-09 20:26:46,172 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:26:46,180 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:26:46,181 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:26:46,188 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f3:3580272+3580273,/Users/user/other/tmp/mapReduce/input/wordCount/f4:0+4194304
     * 2019-10-09 20:26:46,256 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:26:46,256 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:26:46,256 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:26:46,256 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:26:46,256 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:26:46,258 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:26:47,096 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local520479132_0001 running in uber mode : false
     * 2019-10-09 20:26:47,097 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-09 20:26:47,184 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:26:47,185 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:26:47,185 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:26:47,186 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 14073960; bufvoid = 104857600
     * 2019-10-09 20:26:47,186 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 19915020(79660080); length = 6299377/6553600
     * 2019-10-09 20:26:48,099 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:26:48,118 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local520479132_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-09 20:26:48,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:26:48,126 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local520479132_0001_m_000000_0' done.
     * 2019-10-09 20:26:48,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local520479132_0001_m_000000_0
     * 2019-10-09 20:26:48,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local520479132_0001_m_000001_0
     * 2019-10-09 20:26:48,127 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:26:48,128 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:26:48,128 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:26:48,129 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f4:4194304+3272748,/Users/user/other/tmp/mapReduce/input/wordCount/f4:7467052+3272748
     * 2019-10-09 20:26:48,151 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:26:48,151 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:26:48,151 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:26:48,151 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:26:48,151 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:26:48,151 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:26:48,797 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:26:48,797 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:26:48,797 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:26:48,797 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11848811; bufvoid = 104857600
     * 2019-10-09 20:26:48,798 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 20911076(83644304); length = 5303321/6553600
     * 2019-10-09 20:26:49,105 INFO [org.apache.hadoop.mapreduce.Job] -  map 25% reduce 0%
     * 2019-10-09 20:26:49,400 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:26:49,411 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local520479132_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-09 20:26:49,412 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:26:49,412 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local520479132_0001_m_000001_0' done.
     * 2019-10-09 20:26:49,412 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local520479132_0001_m_000001_0
     * 2019-10-09 20:26:49,412 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local520479132_0001_m_000002_0
     * 2019-10-09 20:26:49,414 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:26:49,415 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:26:49,415 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:26:49,416 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f2:2684950+2684950,/Users/user/other/tmp/mapReduce/input/wordCount/f3:0+3580272
     * 2019-10-09 20:26:49,443 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:26:49,443 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:26:49,443 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:26:49,443 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:26:49,443 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:26:49,444 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:26:50,038 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:26:50,038 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:26:50,038 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:26:50,038 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-09 20:26:50,038 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-09 20:26:50,106 INFO [org.apache.hadoop.mapreduce.Job] -  map 50% reduce 0%
     * 2019-10-09 20:26:50,588 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:26:50,598 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local520479132_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-09 20:26:50,600 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:26:50,600 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local520479132_0001_m_000002_0' done.
     * 2019-10-09 20:26:50,600 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local520479132_0001_m_000002_0
     * 2019-10-09 20:26:50,600 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local520479132_0001_m_000003_0
     * 2019-10-09 20:26:50,601 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:26:50,601 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:26:50,601 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:26:50,602 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/wordCount/f0:0+79,/Users/user/other/tmp/mapReduce/input/wordCount/f1:0+1540832,/Users/user/other/tmp/mapReduce/input/wordCount/f2:0+2684950
     * 2019-10-09 20:26:50,625 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 20:26:50,625 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 20:26:50,625 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 20:26:50,625 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 20:26:50,625 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 20:26:50,626 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 20:26:50,927 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 20:26:50,928 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 20:26:50,928 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 20:26:50,928 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649362; bufvoid = 104857600
     * 2019-10-09 20:26:50,928 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790904(91163616); length = 3423493/6553600
     * 2019-10-09 20:26:51,110 INFO [org.apache.hadoop.mapreduce.Job] -  map 75% reduce 0%
     * 2019-10-09 20:26:51,300 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 20:26:51,310 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local520479132_0001_m_000003_0 is done. And is in the process of committing
     * 2019-10-09 20:26:51,311 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-09 20:26:51,311 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local520479132_0001_m_000003_0' done.
     * 2019-10-09 20:26:51,311 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local520479132_0001_m_000003_0
     * 2019-10-09 20:26:51,312 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-09 20:26:51,315 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-09 20:26:51,315 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local520479132_0001_r_000000_0
     * 2019-10-09 20:26:51,323 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 20:26:51,323 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 20:26:51,323 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 20:26:51,325 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@24411723
     * 2019-10-09 20:26:51,338 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-09 20:26:51,340 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local520479132_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-09 20:26:51,389 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local520479132_0001_m_000003_0 decomp: 9361112 len: 9361116 to MEMORY
     * 2019-10-09 20:26:51,406 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9361112 bytes from map-output for attempt_local520479132_0001_m_000003_0
     * 2019-10-09 20:26:51,408 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9361112, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->9361112
     * 2019-10-09 20:26:51,418 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local520479132_0001_m_000000_0 decomp: 17223652 len: 17223656 to MEMORY
     * 2019-10-09 20:26:51,442 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 17223652 bytes from map-output for attempt_local520479132_0001_m_000000_0
     * 2019-10-09 20:26:51,442 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 17223652, inMemoryMapOutputs.size() -> 2, commitMemory -> 9361112, usedMemory ->26584764
     * 2019-10-09 20:26:51,451 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local520479132_0001_m_000001_0 decomp: 14500475 len: 14500479 to MEMORY
     * 2019-10-09 20:26:51,467 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 14500475 bytes from map-output for attempt_local520479132_0001_m_000001_0
     * 2019-10-09 20:26:51,468 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 14500475, inMemoryMapOutputs.size() -> 3, commitMemory -> 26584764, usedMemory ->41085239
     * 2019-10-09 20:26:51,553 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local520479132_0001_m_000002_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-09 20:26:51,569 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local520479132_0001_m_000002_0
     * 2019-10-09 20:26:51,569 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 4, commitMemory -> 41085239, usedMemory ->54964398
     * 2019-10-09 20:26:51,569 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-09 20:26:51,570 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-09 20:26:51,570 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 4 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-09 20:26:51,575 INFO [org.apache.hadoop.mapred.Merger] - Merging 4 sorted segments
     * 2019-10-09 20:26:51,576 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 4 segments left of total size: 54964383 bytes
     * 2019-10-09 20:26:52,111 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-09 20:26:53,267 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 4 segments, 54964398 bytes to disk to satisfy reduce memory limit
     * 2019-10-09 20:26:53,268 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 54964396 bytes from disk
     * 2019-10-09 20:26:53,268 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-09 20:26:53,268 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-09 20:26:53,269 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 54964389 bytes
     * 2019-10-09 20:26:53,269 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-09 20:26:53,281 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-09 20:26:55,055 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local520479132_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-09 20:26:55,056 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-09 20:26:55,056 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local520479132_0001_r_000000_0 is allowed to commit now
     * 2019-10-09 20:26:55,057 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local520479132_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/wordCount/_temporary/0/task_local520479132_0001_r_000000
     * 2019-10-09 20:26:55,058 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-09 20:26:55,058 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local520479132_0001_r_000000_0' done.
     * 2019-10-09 20:26:55,058 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local520479132_0001_r_000000_0
     * 2019-10-09 20:26:55,058 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-09 20:26:55,120 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-09 20:26:55,121 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local520479132_0001 completed successfully
     * 2019-10-09 20:26:55,141 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=202333008
     * 		FILE: Number of bytes written=260832472
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1885025
     * 		Map output records=5025539
     * 		Map output bytes=44913312
     * 		Map output materialized bytes=54964414
     * 		Input split bytes=948
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=12
     * 		Reduce shuffle bytes=54964414
     * 		Reduce input records=5025539
     * 		Reduce output records=12
     * 		Spilled Records=10051078
     * 		Shuffled Maps =4
     * 		Failed Shuffles=0
     * 		Merged Map outputs=4
     * 		GC time elapsed (ms)=396
     * 		Total committed heap usage (bytes)=3203923968
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
