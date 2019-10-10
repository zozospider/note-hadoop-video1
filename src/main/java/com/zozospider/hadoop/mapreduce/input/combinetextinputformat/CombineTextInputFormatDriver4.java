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
public class CombineTextInputFormatDriver4 {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/combinetextinputformat4/
     * total 48488
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:55 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:55 c
     * -rw-r--r--  1 user  staff   6.8M 10 10 13:55 d
     * -rw-r--r--  1 user  staff    10M 10 10 13:55 e
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat4/
     * ls: /Users/user/other/tmp/mapReduce/output/combinetextinputformat4/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/combinetextinputformat4", "/Users/user/other/tmp/mapReduce/output/combinetextinputformat4"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextInputFormatDriver4.class);
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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat4/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 14:13 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10 10 14:13 part-r-00000
     * ➜  output cat combinetextinputformat4/part-r-00000
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
     * 2019-10-10 14:13:40,232 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 14:13:40,490 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 14:13:40,492 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 14:13:40,969 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 14:13:40,984 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 14:13:41,027 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 4
     * 2019-10-10 14:13:41,055 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
     * 2019-10-10 14:13:41,092 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     * 2019-10-10 14:13:41,206 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1823560015_0001
     * 2019-10-10 14:13:41,468 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 14:13:41,470 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1823560015_0001
     * 2019-10-10 14:13:41,470 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 14:13:41,475 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:13:41,478 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 14:13:41,527 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 14:13:41,528 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1823560015_0001_m_000000_0
     * 2019-10-10 14:13:41,562 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:13:41,571 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:13:41,571 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:13:41,577 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/d:3580272+3580273,/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/e:0+4194304
     * 2019-10-10 14:13:41,658 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:13:41,658 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:13:41,658 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:13:41,658 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:13:41,658 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:13:41,660 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:13:42,477 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1823560015_0001 running in uber mode : false
     * 2019-10-10 14:13:42,479 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 14:13:42,600 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:13:42,601 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:13:42,601 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:13:42,601 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 14073960; bufvoid = 104857600
     * 2019-10-10 14:13:42,601 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 19915020(79660080); length = 6299377/6553600
     * 2019-10-10 14:13:43,553 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:13:43,574 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1823560015_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:13:43,583 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:13:43,583 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1823560015_0001_m_000000_0' done.
     * 2019-10-10 14:13:43,583 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1823560015_0001_m_000000_0
     * 2019-10-10 14:13:43,583 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1823560015_0001_m_000001_0
     * 2019-10-10 14:13:43,584 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:13:43,584 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:13:43,584 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:13:43,585 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/e:4194304+3272748,/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/e:7467052+3272748
     * 2019-10-10 14:13:43,607 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:13:43,607 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:13:43,607 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:13:43,607 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:13:43,607 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:13:43,608 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:13:44,266 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:13:44,266 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:13:44,266 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:13:44,266 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11848811; bufvoid = 104857600
     * 2019-10-10 14:13:44,266 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 20911076(83644304); length = 5303321/6553600
     * 2019-10-10 14:13:44,484 INFO [org.apache.hadoop.mapreduce.Job] -  map 25% reduce 0%
     * 2019-10-10 14:13:44,867 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:13:44,886 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1823560015_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 14:13:44,888 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:13:44,888 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1823560015_0001_m_000001_0' done.
     * 2019-10-10 14:13:44,888 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1823560015_0001_m_000001_0
     * 2019-10-10 14:13:44,889 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1823560015_0001_m_000002_0
     * 2019-10-10 14:13:44,889 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:13:44,890 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:13:44,890 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:13:44,891 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/c:2684950+2684950,/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/d:0+3580272
     * 2019-10-10 14:13:44,919 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:13:44,919 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:13:44,919 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:13:44,919 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:13:44,919 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:13:44,919 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:13:45,485 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 14:13:45,562 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:13:45,562 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:13:45,562 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:13:45,562 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-10 14:13:45,562 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-10 14:13:46,124 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:13:46,134 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1823560015_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 14:13:46,135 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:13:46,135 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1823560015_0001_m_000002_0' done.
     * 2019-10-10 14:13:46,135 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1823560015_0001_m_000002_0
     * 2019-10-10 14:13:46,136 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1823560015_0001_m_000003_0
     * 2019-10-10 14:13:46,136 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:13:46,137 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:13:46,137 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:13:46,138 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/b:0+1540832,/Users/user/other/tmp/mapReduce/input/combinetextinputformat4/c:0+2684950
     * 2019-10-10 14:13:46,165 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:13:46,165 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:13:46,165 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:13:46,165 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:13:46,165 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:13:46,166 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:13:46,489 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:13:46,489 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:13:46,489 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:13:46,489 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649219; bufvoid = 104857600
     * 2019-10-10 14:13:46,489 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790968(91163872); length = 3423429/6553600
     * 2019-10-10 14:13:46,490 INFO [org.apache.hadoop.mapreduce.Job] -  map 75% reduce 0%
     * 2019-10-10 14:13:46,851 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:13:46,861 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1823560015_0001_m_000003_0 is done. And is in the process of committing
     * 2019-10-10 14:13:46,863 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:13:46,863 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1823560015_0001_m_000003_0' done.
     * 2019-10-10 14:13:46,864 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1823560015_0001_m_000003_0
     * 2019-10-10 14:13:46,864 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 14:13:46,866 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 14:13:46,867 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1823560015_0001_r_000000_0
     * 2019-10-10 14:13:46,874 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:13:46,875 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:13:46,875 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:13:46,877 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6052676
     * 2019-10-10 14:13:46,889 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 14:13:46,891 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1823560015_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 14:13:46,940 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1823560015_0001_m_000001_0 decomp: 14500475 len: 14500479 to MEMORY
     * 2019-10-10 14:13:46,964 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 14500475 bytes from map-output for attempt_local1823560015_0001_m_000001_0
     * 2019-10-10 14:13:46,965 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 14500475, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->14500475
     * 2019-10-10 14:13:46,976 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1823560015_0001_m_000000_0 decomp: 17223652 len: 17223656 to MEMORY
     * 2019-10-10 14:13:47,002 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 17223652 bytes from map-output for attempt_local1823560015_0001_m_000000_0
     * 2019-10-10 14:13:47,002 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 17223652, inMemoryMapOutputs.size() -> 2, commitMemory -> 14500475, usedMemory ->31724127
     * 2019-10-10 14:13:47,007 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1823560015_0001_m_000003_0 decomp: 9360937 len: 9360941 to MEMORY
     * 2019-10-10 14:13:47,018 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9360937 bytes from map-output for attempt_local1823560015_0001_m_000003_0
     * 2019-10-10 14:13:47,018 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9360937, inMemoryMapOutputs.size() -> 3, commitMemory -> 31724127, usedMemory ->41085064
     * 2019-10-10 14:13:47,088 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1823560015_0001_m_000002_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-10 14:13:47,106 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local1823560015_0001_m_000002_0
     * 2019-10-10 14:13:47,106 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 4, commitMemory -> 41085064, usedMemory ->54964223
     * 2019-10-10 14:13:47,106 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 14:13:47,107 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 14:13:47,107 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 4 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 14:13:47,114 INFO [org.apache.hadoop.mapred.Merger] - Merging 4 sorted segments
     * 2019-10-10 14:13:47,114 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 4 segments left of total size: 54964208 bytes
     * 2019-10-10 14:13:47,495 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 14:13:48,659 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 4 segments, 54964223 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 14:13:48,659 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 54964221 bytes from disk
     * 2019-10-10 14:13:48,660 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 14:13:48,660 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 14:13:48,660 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 54964214 bytes
     * 2019-10-10 14:13:48,661 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 14:13:48,711 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 14:13:50,444 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1823560015_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:13:50,446 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 14:13:50,446 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1823560015_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 14:13:50,447 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1823560015_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/combinetextinputformat4/_temporary/0/task_local1823560015_0001_r_000000
     * 2019-10-10 14:13:50,447 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 14:13:50,447 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1823560015_0001_r_000000_0' done.
     * 2019-10-10 14:13:50,447 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1823560015_0001_r_000000_0
     * 2019-10-10 14:13:50,448 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 14:13:50,507 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 14:13:50,507 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1823560015_0001 completed successfully
     * 2019-10-10 14:13:50,523 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=202332948
     * 		FILE: Number of bytes written=260840527
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1885019
     * 		Map output records=5025523
     * 		Map output bytes=44913169
     * 		Map output materialized bytes=54964239
     * 		Input split bytes=980
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
     * 		GC time elapsed (ms)=392
     * 		Total committed heap usage (bytes)=3202351104
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
