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
public class CombineTextDriver4 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/CombineText4/
     * total 48488
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:55 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:55 c
     * -rw-r--r--  1 user  staff   6.8M 10 10 13:55 d
     * -rw-r--r--  1 user  staff    10M 10 10 13:55 e
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText4/
     * ls: /Users/user/other/tmp/MapReduce/output/CombineText4/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/CombineText4", "/Users/user/other/tmp/MapReduce/output/CombineText4"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextDriver4.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText4/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 16:19 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10 10 16:19 part-r-00000
     * ➜  output cat CombineText4/part-r-00000
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
     * 2019-10-10 16:19:00,274 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 16:19:00,567 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 16:19:00,569 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 16:19:01,194 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 16:19:01,199 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 16:19:01,231 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 4
     * 2019-10-10 16:19:01,250 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
     * 2019-10-10 16:19:01,287 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     * 2019-10-10 16:19:01,388 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1038609765_0001
     * 2019-10-10 16:19:01,595 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 16:19:01,596 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1038609765_0001
     * 2019-10-10 16:19:01,600 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 16:19:01,605 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:01,608 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 16:19:01,662 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 16:19:01,664 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1038609765_0001_m_000000_0
     * 2019-10-10 16:19:01,692 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:01,700 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:01,700 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:01,706 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText4/d:3580272+3580273,/Users/user/other/tmp/MapReduce/input/CombineText4/e:0+4194304
     * 2019-10-10 16:19:01,781 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:01,781 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:01,781 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:01,781 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:01,781 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:01,783 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:02,600 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1038609765_0001 running in uber mode : false
     * 2019-10-10 16:19:02,601 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 16:19:02,787 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:02,787 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:02,787 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:02,787 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 14073960; bufvoid = 104857600
     * 2019-10-10 16:19:02,787 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 19915020(79660080); length = 6299377/6553600
     * 2019-10-10 16:19:03,672 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:03,693 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1038609765_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:19:03,702 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:03,703 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1038609765_0001_m_000000_0' done.
     * 2019-10-10 16:19:03,703 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1038609765_0001_m_000000_0
     * 2019-10-10 16:19:03,703 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1038609765_0001_m_000001_0
     * 2019-10-10 16:19:03,704 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:03,705 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:03,705 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:03,706 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText4/e:4194304+3272748,/Users/user/other/tmp/MapReduce/input/CombineText4/e:7467052+3272748
     * 2019-10-10 16:19:03,741 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:03,741 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:03,741 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:03,741 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:03,741 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:03,742 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:04,565 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:04,566 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:04,566 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:04,566 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11848811; bufvoid = 104857600
     * 2019-10-10 16:19:04,566 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 20911076(83644304); length = 5303321/6553600
     * 2019-10-10 16:19:04,610 INFO [org.apache.hadoop.mapreduce.Job] -  map 25% reduce 0%
     * 2019-10-10 16:19:05,145 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:05,160 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1038609765_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 16:19:05,162 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:05,162 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1038609765_0001_m_000001_0' done.
     * 2019-10-10 16:19:05,162 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1038609765_0001_m_000001_0
     * 2019-10-10 16:19:05,162 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1038609765_0001_m_000002_0
     * 2019-10-10 16:19:05,163 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:05,163 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:05,163 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:05,164 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText4/c:2684950+2684950,/Users/user/other/tmp/MapReduce/input/CombineText4/d:0+3580272
     * 2019-10-10 16:19:05,184 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:05,184 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:05,184 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:05,184 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:05,184 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:05,185 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:05,743 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 16:19:05,975 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:05,975 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:05,975 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:05,975 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-10 16:19:05,975 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-10 16:19:06,527 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:06,541 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1038609765_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 16:19:06,544 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:06,544 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1038609765_0001_m_000002_0' done.
     * 2019-10-10 16:19:06,544 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1038609765_0001_m_000002_0
     * 2019-10-10 16:19:06,544 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1038609765_0001_m_000003_0
     * 2019-10-10 16:19:06,545 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:06,545 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:06,545 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:06,547 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText4/b:0+1540832,/Users/user/other/tmp/MapReduce/input/CombineText4/c:0+2684950
     * 2019-10-10 16:19:06,572 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:06,572 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:06,572 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:06,572 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:06,572 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:06,572 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:07,026 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:07,026 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:07,026 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:07,026 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649219; bufvoid = 104857600
     * 2019-10-10 16:19:07,026 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790968(91163872); length = 3423429/6553600
     * 2019-10-10 16:19:07,384 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:07,398 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1038609765_0001_m_000003_0 is done. And is in the process of committing
     * 2019-10-10 16:19:07,399 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:07,399 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1038609765_0001_m_000003_0' done.
     * 2019-10-10 16:19:07,399 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1038609765_0001_m_000003_0
     * 2019-10-10 16:19:07,400 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 16:19:07,402 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 16:19:07,402 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1038609765_0001_r_000000_0
     * 2019-10-10 16:19:07,408 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:07,409 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:07,409 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:07,411 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@25643db8
     * 2019-10-10 16:19:07,424 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 16:19:07,425 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1038609765_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 16:19:07,465 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1038609765_0001_m_000001_0 decomp: 14500475 len: 14500479 to MEMORY
     * 2019-10-10 16:19:07,491 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 14500475 bytes from map-output for attempt_local1038609765_0001_m_000001_0
     * 2019-10-10 16:19:07,492 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 14500475, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->14500475
     * 2019-10-10 16:19:07,498 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1038609765_0001_m_000000_0 decomp: 17223652 len: 17223656 to MEMORY
     * 2019-10-10 16:19:07,530 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 17223652 bytes from map-output for attempt_local1038609765_0001_m_000000_0
     * 2019-10-10 16:19:07,530 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 17223652, inMemoryMapOutputs.size() -> 2, commitMemory -> 14500475, usedMemory ->31724127
     * 2019-10-10 16:19:07,534 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1038609765_0001_m_000003_0 decomp: 9360937 len: 9360941 to MEMORY
     * 2019-10-10 16:19:07,545 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9360937 bytes from map-output for attempt_local1038609765_0001_m_000003_0
     * 2019-10-10 16:19:07,545 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9360937, inMemoryMapOutputs.size() -> 3, commitMemory -> 31724127, usedMemory ->41085064
     * 2019-10-10 16:19:07,549 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1038609765_0001_m_000002_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-10 16:19:07,566 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local1038609765_0001_m_000002_0
     * 2019-10-10 16:19:07,566 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 4, commitMemory -> 41085064, usedMemory ->54964223
     * 2019-10-10 16:19:07,566 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 16:19:07,567 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 16:19:07,567 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 4 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 16:19:07,573 INFO [org.apache.hadoop.mapred.Merger] - Merging 4 sorted segments
     * 2019-10-10 16:19:07,574 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 4 segments left of total size: 54964208 bytes
     * 2019-10-10 16:19:09,148 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 4 segments, 54964223 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 16:19:09,148 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 54964221 bytes from disk
     * 2019-10-10 16:19:09,149 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 16:19:09,149 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:19:09,149 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 54964214 bytes
     * 2019-10-10 16:19:09,149 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 16:19:09,161 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 16:19:10,887 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1038609765_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:19:10,888 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 16:19:10,888 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1038609765_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 16:19:10,889 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1038609765_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/CombineText4/_temporary/0/task_local1038609765_0001_r_000000
     * 2019-10-10 16:19:10,889 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 16:19:10,889 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1038609765_0001_r_000000_0' done.
     * 2019-10-10 16:19:10,890 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1038609765_0001_r_000000_0
     * 2019-10-10 16:19:10,890 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 16:19:11,755 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 16:19:11,755 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1038609765_0001 completed successfully
     * 2019-10-10 16:19:11,768 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=202331716
     * 		FILE: Number of bytes written=260839427
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1885019
     * 		Map output records=5025523
     * 		Map output bytes=44913169
     * 		Map output materialized bytes=54964239
     * 		Input split bytes=892
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
     * 		GC time elapsed (ms)=443
     * 		Total committed heap usage (bytes)=3876585472
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
