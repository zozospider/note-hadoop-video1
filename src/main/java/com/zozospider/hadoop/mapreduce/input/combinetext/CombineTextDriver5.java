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
public class CombineTextDriver5 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/CombineText5/
     * total 48496
     * -rw-r--r--  1 user  staff    79B 10 10 13:59 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:59 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:59 c
     * -rw-r--r--  1 user  staff   6.8M 10 10 13:59 d
     * -rw-r--r--  1 user  staff    10M 10 10 13:59 e
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText5/
     * ls: /Users/user/other/tmp/MapReduce/output/CombineText5/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/CombineText5", "/Users/user/other/tmp/MapReduce/output/CombineText5"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextDriver5.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText5/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 16:19 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10 10 16:19 part-r-00000
     * ➜  output cat CombineText5/part-r-00000
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
     * 2019-10-10 16:19:43,300 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 16:19:43,501 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 16:19:43,502 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 16:19:44,072 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 16:19:44,079 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 16:19:44,139 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 5
     * 2019-10-10 16:19:44,169 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
     * 2019-10-10 16:19:44,227 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     * 2019-10-10 16:19:44,351 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1858108225_0001
     * 2019-10-10 16:19:44,564 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 16:19:44,566 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1858108225_0001
     * 2019-10-10 16:19:44,566 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 16:19:44,572 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:44,574 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 16:19:44,625 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 16:19:44,625 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1858108225_0001_m_000000_0
     * 2019-10-10 16:19:44,655 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:44,662 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:44,662 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:44,669 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText5/d:3580272+3580273,/Users/user/other/tmp/MapReduce/input/CombineText5/e:0+4194304
     * 2019-10-10 16:19:44,748 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:44,748 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:44,748 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:44,748 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:44,748 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:44,750 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:45,569 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1858108225_0001 running in uber mode : false
     * 2019-10-10 16:19:45,570 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 16:19:45,678 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:45,678 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:45,678 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:45,678 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 14073960; bufvoid = 104857600
     * 2019-10-10 16:19:45,680 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 19915020(79660080); length = 6299377/6553600
     * 2019-10-10 16:19:46,605 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:46,628 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1858108225_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:19:46,637 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:46,638 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1858108225_0001_m_000000_0' done.
     * 2019-10-10 16:19:46,638 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1858108225_0001_m_000000_0
     * 2019-10-10 16:19:46,638 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1858108225_0001_m_000001_0
     * 2019-10-10 16:19:46,639 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:46,639 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:46,639 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:46,640 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText5/e:4194304+3272748,/Users/user/other/tmp/MapReduce/input/CombineText5/e:7467052+3272748
     * 2019-10-10 16:19:46,660 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:46,660 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:46,660 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:46,660 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:46,660 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:46,661 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:47,317 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:47,317 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:47,317 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:47,317 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11848811; bufvoid = 104857600
     * 2019-10-10 16:19:47,317 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 20911076(83644304); length = 5303321/6553600
     * 2019-10-10 16:19:47,579 INFO [org.apache.hadoop.mapreduce.Job] -  map 25% reduce 0%
     * 2019-10-10 16:19:47,893 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:47,912 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1858108225_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 16:19:47,914 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:47,914 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1858108225_0001_m_000001_0' done.
     * 2019-10-10 16:19:47,914 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1858108225_0001_m_000001_0
     * 2019-10-10 16:19:47,914 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1858108225_0001_m_000002_0
     * 2019-10-10 16:19:47,915 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:47,916 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:47,916 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:47,917 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText5/c:2684950+2684950,/Users/user/other/tmp/MapReduce/input/CombineText5/d:0+3580272
     * 2019-10-10 16:19:47,943 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:47,944 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:47,944 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:47,944 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:47,944 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:47,944 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:48,584 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 16:19:48,588 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:48,588 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:48,588 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:48,588 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-10 16:19:48,588 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-10 16:19:49,125 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:49,139 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1858108225_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 16:19:49,140 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:49,140 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1858108225_0001_m_000002_0' done.
     * 2019-10-10 16:19:49,140 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1858108225_0001_m_000002_0
     * 2019-10-10 16:19:49,140 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1858108225_0001_m_000003_0
     * 2019-10-10 16:19:49,141 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:49,142 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:49,142 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:49,143 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText5/a:0+79,/Users/user/other/tmp/MapReduce/input/CombineText5/b:0+1540832,/Users/user/other/tmp/MapReduce/input/CombineText5/c:0+2684950
     * 2019-10-10 16:19:49,166 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:19:49,167 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:19:49,167 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:19:49,167 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:19:49,167 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:19:49,167 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:19:49,516 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:19:49,516 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:19:49,516 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:19:49,516 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649362; bufvoid = 104857600
     * 2019-10-10 16:19:49,516 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790904(91163616); length = 3423493/6553600
     * 2019-10-10 16:19:49,590 INFO [org.apache.hadoop.mapreduce.Job] -  map 75% reduce 0%
     * 2019-10-10 16:19:49,868 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:19:49,878 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1858108225_0001_m_000003_0 is done. And is in the process of committing
     * 2019-10-10 16:19:49,880 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:19:49,880 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1858108225_0001_m_000003_0' done.
     * 2019-10-10 16:19:49,880 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1858108225_0001_m_000003_0
     * 2019-10-10 16:19:49,880 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 16:19:49,882 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 16:19:49,883 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1858108225_0001_r_000000_0
     * 2019-10-10 16:19:49,891 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:19:49,892 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:19:49,892 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:19:49,895 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@4dada024
     * 2019-10-10 16:19:49,907 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 16:19:49,910 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1858108225_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 16:19:49,957 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1858108225_0001_m_000003_0 decomp: 9361112 len: 9361116 to MEMORY
     * 2019-10-10 16:19:49,974 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9361112 bytes from map-output for attempt_local1858108225_0001_m_000003_0
     * 2019-10-10 16:19:49,975 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9361112, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->9361112
     * 2019-10-10 16:19:49,986 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1858108225_0001_m_000000_0 decomp: 17223652 len: 17223656 to MEMORY
     * 2019-10-10 16:19:50,010 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 17223652 bytes from map-output for attempt_local1858108225_0001_m_000000_0
     * 2019-10-10 16:19:50,010 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 17223652, inMemoryMapOutputs.size() -> 2, commitMemory -> 9361112, usedMemory ->26584764
     * 2019-10-10 16:19:50,018 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1858108225_0001_m_000002_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-10 16:19:50,034 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local1858108225_0001_m_000002_0
     * 2019-10-10 16:19:50,034 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 3, commitMemory -> 26584764, usedMemory ->40463923
     * 2019-10-10 16:19:50,042 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1858108225_0001_m_000001_0 decomp: 14500475 len: 14500479 to MEMORY
     * 2019-10-10 16:19:50,063 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 14500475 bytes from map-output for attempt_local1858108225_0001_m_000001_0
     * 2019-10-10 16:19:50,063 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 14500475, inMemoryMapOutputs.size() -> 4, commitMemory -> 40463923, usedMemory ->54964398
     * 2019-10-10 16:19:50,064 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 16:19:50,065 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 16:19:50,155 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 4 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 16:19:50,162 INFO [org.apache.hadoop.mapred.Merger] - Merging 4 sorted segments
     * 2019-10-10 16:19:50,162 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 4 segments left of total size: 54964383 bytes
     * 2019-10-10 16:19:50,592 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 16:19:51,680 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 4 segments, 54964398 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 16:19:51,681 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 54964396 bytes from disk
     * 2019-10-10 16:19:51,681 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 16:19:51,681 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:19:51,682 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 54964389 bytes
     * 2019-10-10 16:19:51,682 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 16:19:51,693 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 16:19:53,335 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1858108225_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:19:53,336 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 16:19:53,336 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1858108225_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 16:19:53,336 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1858108225_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/CombineText5/_temporary/0/task_local1858108225_0001_r_000000
     * 2019-10-10 16:19:53,337 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 16:19:53,337 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1858108225_0001_r_000000_0' done.
     * 2019-10-10 16:19:53,337 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1858108225_0001_r_000000_0
     * 2019-10-10 16:19:53,337 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 16:19:53,602 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 16:19:53,602 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1858108225_0001 completed successfully
     * 2019-10-10 16:19:53,617 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=202333260
     * 		FILE: Number of bytes written=260840322
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1885025
     * 		Map output records=5025539
     * 		Map output bytes=44913312
     * 		Map output materialized bytes=54964414
     * 		Input split bytes=966
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
     * 		GC time elapsed (ms)=409
     * 		Total committed heap usage (bytes)=3230662656
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
