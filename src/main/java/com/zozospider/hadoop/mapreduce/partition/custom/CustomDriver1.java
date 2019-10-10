package com.zozospider.hadoop.mapreduce.partition.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动 (标准方式)
 */
public class CustomDriver1 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/partition/Custom1/
     * total 24
     * -rw-r--r--  1 user  staff    54B 10 10 20:27 f1
     * -rw-r--r--  1 user  staff    59B 10 10 20:27 f2
     * -rw-r--r--  1 user  staff    35B 10 10 20:27 f3
     * ➜  input cat partition/Custom1/f1
     * abc abc love
     * qq
     * love love qq
     * qq love
     * google book book
     * ➜  input cat partition/Custom1/f2
     * love love
     * qq google google
     * abc abc qq book
     * book book
     * do do
     * ➜  input cat partition/Custom1/f3
     * love qq
     * google abc
     * book book do do
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom1/
     * ls: /Users/user/other/tmp/MapReduce/output/partition/Custom1/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/partition/Custom1", "/Users/user/other/tmp/MapReduce/output/partition/Custom1"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Partitioner 为 CustomPartitioner
        job.setPartitionerClass(CustomPartitioner.class);
        // 设置 ReduceTasks 个数为 4 (标准方式: 等于 CustomPartitioner 中定义的 partitions 个数)
        job.setNumReduceTasks(4);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CustomDriver1.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom1/
     * total 24
     * -rw-r--r--  1 user  staff     0B 10 10 20:33 _SUCCESS
     * -rw-r--r--  1 user  staff    18B 10 10 20:33 part-r-00000
     * -rw-r--r--  1 user  staff    16B 10 10 20:33 part-r-00001
     * -rw-r--r--  1 user  staff     0B 10 10 20:33 part-r-00002
     * -rw-r--r--  1 user  staff     5B 10 10 20:33 part-r-00003
     * ➜  output cat partition/Custom1/part-r-00000
     * abc	5
     * book	7
     * do	4
     * ➜  output cat partition/Custom1/part-r-00001
     * google	4
     * love	7
     * ➜  output cat partition/Custom1/part-r-00002
     * ➜  output cat partition/Custom1/part-r-00003
     * qq	6
     * ➜  output
     */

    /**
     * 2019-10-10 20:33:15,608 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 20:33:15,970 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 20:33:15,971 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 20:33:16,424 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 20:33:16,430 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 20:33:16,470 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 20:33:16,527 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 20:33:16,656 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local401962858_0001
     * 2019-10-10 20:33:16,873 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 20:33:16,875 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local401962858_0001
     * 2019-10-10 20:33:16,875 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 20:33:16,881 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:16,883 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 20:33:16,939 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 20:33:16,939 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_m_000000_0
     * 2019-10-10 20:33:16,971 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:16,979 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:16,980 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:16,986 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom1/f2:0+59
     * 2019-10-10 20:33:17,054 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:33:17,054 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:33:17,054 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:33:17,054 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:33:17,054 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:33:17,056 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:33:17,063 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:33:17,064 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:33:17,064 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:33:17,064 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 111; bufvoid = 104857600
     * 2019-10-10 20:33:17,064 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214348(104857392); length = 49/6553600
     * 2019-10-10 20:33:17,072 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:33:17,075 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,083 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:33:17,083 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_m_000000_0' done.
     * 2019-10-10 20:33:17,083 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_m_000000_0
     * 2019-10-10 20:33:17,083 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_m_000001_0
     * 2019-10-10 20:33:17,085 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:17,086 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:17,086 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:17,088 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom1/f1:0+54
     * 2019-10-10 20:33:17,141 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:33:17,141 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:33:17,141 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:33:17,141 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:33:17,141 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:33:17,141 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:33:17,143 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:33:17,143 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:33:17,143 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:33:17,143 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 102; bufvoid = 104857600
     * 2019-10-10 20:33:17,143 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214352(104857408); length = 45/6553600
     * 2019-10-10 20:33:17,145 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:33:17,147 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,149 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:33:17,149 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_m_000001_0' done.
     * 2019-10-10 20:33:17,149 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_m_000001_0
     * 2019-10-10 20:33:17,149 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_m_000002_0
     * 2019-10-10 20:33:17,150 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:17,151 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:17,151 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:17,153 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom1/f3:0+35
     * 2019-10-10 20:33:17,209 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:33:17,209 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:33:17,209 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:33:17,209 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:33:17,209 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:33:17,210 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:33:17,211 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:33:17,211 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:33:17,211 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:33:17,211 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 67; bufvoid = 104857600
     * 2019-10-10 20:33:17,211 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214368(104857472); length = 29/6553600
     * 2019-10-10 20:33:17,213 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:33:17,215 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,216 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:33:17,217 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_m_000002_0' done.
     * 2019-10-10 20:33:17,217 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_m_000002_0
     * 2019-10-10 20:33:17,217 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 20:33:17,221 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 20:33:17,221 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_r_000000_0
     * 2019-10-10 20:33:17,228 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:17,228 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:17,228 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:17,232 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@e797256
     * 2019-10-10 20:33:17,246 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:33:17,249 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local401962858_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:33:17,293 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local401962858_0001_m_000001_0 decomp: 44 len: 48 to MEMORY
     * 2019-10-10 20:33:17,308 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 44 bytes from map-output for attempt_local401962858_0001_m_000001_0
     * 2019-10-10 20:33:17,311 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 44, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->44
     * 2019-10-10 20:33:17,329 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local401962858_0001_m_000002_0 decomp: 52 len: 56 to MEMORY
     * 2019-10-10 20:33:17,330 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 52 bytes from map-output for attempt_local401962858_0001_m_000002_0
     * 2019-10-10 20:33:17,330 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 52, inMemoryMapOutputs.size() -> 2, commitMemory -> 44, usedMemory ->96
     * 2019-10-10 20:33:17,332 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local401962858_0001_m_000000_0 decomp: 73 len: 77 to MEMORY
     * 2019-10-10 20:33:17,333 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 73 bytes from map-output for attempt_local401962858_0001_m_000000_0
     * 2019-10-10 20:33:17,333 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 73, inMemoryMapOutputs.size() -> 3, commitMemory -> 96, usedMemory ->169
     * 2019-10-10 20:33:17,334 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:33:17,336 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,338 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:33:17,345 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:33:17,345 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 151 bytes
     * 2019-10-10 20:33:17,347 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 169 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:33:17,347 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 169 bytes from disk
     * 2019-10-10 20:33:17,348 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:33:17,348 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:33:17,349 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 159 bytes
     * 2019-10-10 20:33:17,349 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,369 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 20:33:17,377 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,378 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,379 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local401962858_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 20:33:17,380 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local401962858_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom1/_temporary/0/task_local401962858_0001_r_000000
     * 2019-10-10 20:33:17,380 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:33:17,381 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_r_000000_0' done.
     * 2019-10-10 20:33:17,381 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_r_000000_0
     * 2019-10-10 20:33:17,381 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_r_000001_0
     * 2019-10-10 20:33:17,382 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:17,383 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:17,383 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:17,383 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@1c2c95df
     * 2019-10-10 20:33:17,384 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:33:17,385 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local401962858_0001_r_000001_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:33:17,388 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local401962858_0001_m_000001_0 decomp: 59 len: 63 to MEMORY
     * 2019-10-10 20:33:17,389 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 59 bytes from map-output for attempt_local401962858_0001_m_000001_0
     * 2019-10-10 20:33:17,389 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 59, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->59
     * 2019-10-10 20:33:17,391 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local401962858_0001_m_000002_0 decomp: 26 len: 30 to MEMORY
     * 2019-10-10 20:33:17,391 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 26 bytes from map-output for attempt_local401962858_0001_m_000002_0
     * 2019-10-10 20:33:17,391 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 26, inMemoryMapOutputs.size() -> 2, commitMemory -> 59, usedMemory ->85
     * 2019-10-10 20:33:17,393 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local401962858_0001_m_000000_0 decomp: 50 len: 54 to MEMORY
     * 2019-10-10 20:33:17,394 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 50 bytes from map-output for attempt_local401962858_0001_m_000000_0
     * 2019-10-10 20:33:17,394 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 50, inMemoryMapOutputs.size() -> 3, commitMemory -> 85, usedMemory ->135
     * 2019-10-10 20:33:17,397 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:33:17,397 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,398 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:33:17,400 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:33:17,403 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 108 bytes
     * 2019-10-10 20:33:17,404 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 135 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:33:17,404 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 135 bytes from disk
     * 2019-10-10 20:33:17,404 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:33:17,404 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:33:17,405 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 122 bytes
     * 2019-10-10 20:33:17,406 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,422 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_r_000001_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,424 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,424 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local401962858_0001_r_000001_0 is allowed to commit now
     * 2019-10-10 20:33:17,425 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local401962858_0001_r_000001_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom1/_temporary/0/task_local401962858_0001_r_000001
     * 2019-10-10 20:33:17,426 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:33:17,426 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_r_000001_0' done.
     * 2019-10-10 20:33:17,426 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_r_000001_0
     * 2019-10-10 20:33:17,426 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_r_000002_0
     * 2019-10-10 20:33:17,427 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:17,428 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:17,428 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:17,428 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@586f4031
     * 2019-10-10 20:33:17,428 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:33:17,429 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local401962858_0001_r_000002_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:33:17,431 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#3 about to shuffle output of map attempt_local401962858_0001_m_000001_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:33:17,432 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local401962858_0001_m_000001_0
     * 2019-10-10 20:33:17,432 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->2
     * 2019-10-10 20:33:17,433 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#3 about to shuffle output of map attempt_local401962858_0001_m_000002_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:33:17,434 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local401962858_0001_m_000002_0
     * 2019-10-10 20:33:17,434 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 2, commitMemory -> 2, usedMemory ->4
     * 2019-10-10 20:33:17,435 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#3 about to shuffle output of map attempt_local401962858_0001_m_000000_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:33:17,436 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local401962858_0001_m_000000_0
     * 2019-10-10 20:33:17,436 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 3, commitMemory -> 4, usedMemory ->6
     * 2019-10-10 20:33:17,437 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:33:17,437 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,437 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:33:17,439 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:33:17,439 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:33:17,439 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 6 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:33:17,440 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 6 bytes from disk
     * 2019-10-10 20:33:17,440 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:33:17,440 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:33:17,440 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:33:17,441 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,459 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_r_000002_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,460 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,460 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local401962858_0001_r_000002_0 is allowed to commit now
     * 2019-10-10 20:33:17,461 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local401962858_0001_r_000002_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom1/_temporary/0/task_local401962858_0001_r_000002
     * 2019-10-10 20:33:17,462 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:33:17,462 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_r_000002_0' done.
     * 2019-10-10 20:33:17,462 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_r_000002_0
     * 2019-10-10 20:33:17,462 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local401962858_0001_r_000003_0
     * 2019-10-10 20:33:17,463 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:33:17,463 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:33:17,464 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:33:17,464 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@1503522
     * 2019-10-10 20:33:17,464 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:33:17,465 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local401962858_0001_r_000003_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:33:17,467 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#4 about to shuffle output of map attempt_local401962858_0001_m_000001_0 decomp: 29 len: 33 to MEMORY
     * 2019-10-10 20:33:17,467 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 29 bytes from map-output for attempt_local401962858_0001_m_000001_0
     * 2019-10-10 20:33:17,468 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 29, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->29
     * 2019-10-10 20:33:17,469 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#4 about to shuffle output of map attempt_local401962858_0001_m_000002_0 decomp: 11 len: 15 to MEMORY
     * 2019-10-10 20:33:17,470 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 11 bytes from map-output for attempt_local401962858_0001_m_000002_0
     * 2019-10-10 20:33:17,470 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 11, inMemoryMapOutputs.size() -> 2, commitMemory -> 29, usedMemory ->40
     * 2019-10-10 20:33:17,472 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#4 about to shuffle output of map attempt_local401962858_0001_m_000000_0 decomp: 20 len: 24 to MEMORY
     * 2019-10-10 20:33:17,472 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 20 bytes from map-output for attempt_local401962858_0001_m_000000_0
     * 2019-10-10 20:33:17,472 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 20, inMemoryMapOutputs.size() -> 3, commitMemory -> 40, usedMemory ->60
     * 2019-10-10 20:33:17,472 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:33:17,473 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,473 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:33:17,474 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:33:17,474 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 45 bytes
     * 2019-10-10 20:33:17,475 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 60 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:33:17,475 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 60 bytes from disk
     * 2019-10-10 20:33:17,475 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:33:17,475 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:33:17,475 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 51 bytes
     * 2019-10-10 20:33:17,476 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,488 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local401962858_0001_r_000003_0 is done. And is in the process of committing
     * 2019-10-10 20:33:17,489 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:33:17,489 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local401962858_0001_r_000003_0 is allowed to commit now
     * 2019-10-10 20:33:17,490 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local401962858_0001_r_000003_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom1/_temporary/0/task_local401962858_0001_r_000003
     * 2019-10-10 20:33:17,490 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:33:17,490 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local401962858_0001_r_000003_0' done.
     * 2019-10-10 20:33:17,491 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local401962858_0001_r_000003_0
     * 2019-10-10 20:33:17,491 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 20:33:17,878 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local401962858_0001 running in uber mode : false
     * 2019-10-10 20:33:17,879 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 20:33:17,880 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local401962858_0001 completed successfully
     * 2019-10-10 20:33:17,896 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=15522
     * 		FILE: Number of bytes written=1944537
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=13
     * 		Map output records=33
     * 		Map output bytes=280
     * 		Map output materialized bytes=418
     * 		Input split bytes=384
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=6
     * 		Reduce shuffle bytes=418
     * 		Reduce input records=33
     * 		Reduce output records=6
     * 		Spilled Records=66
     * 		Shuffled Maps =12
     * 		Failed Shuffles=0
     * 		Merged Map outputs=12
     * 		GC time elapsed (ms)=16
     * 		Total committed heap usage (bytes)=2799697920
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=148
     * 	File Output Format Counters
     * 		Bytes Written=83
     *
     * Process finished with exit code 0
     */

}
