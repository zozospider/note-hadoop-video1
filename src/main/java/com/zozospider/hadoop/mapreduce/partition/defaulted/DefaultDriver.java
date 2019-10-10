package com.zozospider.hadoop.mapreduce.partition.defaulted;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动
 */
public class DefaultDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/partition/Default/
     * total 24
     * -rw-r--r--  1 user  staff    54B 10 10 19:56 f1
     * -rw-r--r--  1 user  staff    59B 10 10 19:56 f2
     * -rw-r--r--  1 user  staff    35B 10 10 19:56 f3
     * ➜  input cat partition/Default/f1
     * abc abc love
     * qq
     * love love qq
     * qq love
     * google book book
     * ➜  input cat partition/Default/f2
     * love love
     * qq google google
     * abc abc qq book
     * book book
     * do do
     * ➜  input cat partition/Default/f3
     * love qq
     * google abc
     * book book do do
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Default/
     * ls: /Users/user/other/tmp/MapReduce/output/partition/Default/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/partition/Default", "/Users/user/other/tmp/MapReduce/output/partition/Default"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 ReduceTasks 个数为 2
        job.setNumReduceTasks(2);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(DefaultDriver.class);
        job.setMapperClass(DefaultMapper.class);
        job.setReducerClass(DefaultReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Default/
     * total 16
     * -rw-r--r--  1 user  staff     0B 10 10 19:57 _SUCCESS
     * -rw-r--r--  1 user  staff    21B 10 10 19:57 part-r-00000
     * -rw-r--r--  1 user  staff    18B 10 10 19:57 part-r-00001
     * ➜  output cat partition/Default/part-r-00000
     * book	7
     * do	4
     * google	4
     * ➜  output cat partition/Default/part-r-00001
     * abc	5
     * love	7
     * qq	6
     * ➜  output
     */

    /**
     * 2019-10-10 19:57:30,957 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 19:57:31,299 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 19:57:31,300 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 19:57:31,966 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 19:57:31,972 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 19:57:32,008 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 19:57:32,059 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 19:57:32,177 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local121577074_0001
     * 2019-10-10 19:57:32,403 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 19:57:32,404 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local121577074_0001
     * 2019-10-10 19:57:32,404 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 19:57:32,410 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 19:57:32,412 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 19:57:32,470 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 19:57:32,470 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local121577074_0001_m_000000_0
     * 2019-10-10 19:57:32,502 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 19:57:32,519 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 19:57:32,520 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 19:57:32,526 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Default/f2:0+59
     * 2019-10-10 19:57:32,597 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 19:57:32,597 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 19:57:32,597 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 19:57:32,597 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 19:57:32,597 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 19:57:32,599 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 19:57:32,607 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 19:57:32,608 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 19:57:32,608 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 19:57:32,608 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 111; bufvoid = 104857600
     * 2019-10-10 19:57:32,608 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214348(104857392); length = 49/6553600
     * 2019-10-10 19:57:32,619 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 19:57:32,624 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local121577074_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 19:57:32,634 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 19:57:32,634 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local121577074_0001_m_000000_0' done.
     * 2019-10-10 19:57:32,634 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local121577074_0001_m_000000_0
     * 2019-10-10 19:57:32,634 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local121577074_0001_m_000001_0
     * 2019-10-10 19:57:32,636 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 19:57:32,637 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 19:57:32,637 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 19:57:32,638 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Default/f1:0+54
     * 2019-10-10 19:57:32,691 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 19:57:32,691 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 19:57:32,691 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 19:57:32,691 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 19:57:32,691 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 19:57:32,692 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 19:57:32,693 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 19:57:32,693 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 19:57:32,693 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 19:57:32,693 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 102; bufvoid = 104857600
     * 2019-10-10 19:57:32,693 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214352(104857408); length = 45/6553600
     * 2019-10-10 19:57:32,696 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 19:57:32,703 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local121577074_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 19:57:32,716 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 19:57:32,717 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local121577074_0001_m_000001_0' done.
     * 2019-10-10 19:57:32,717 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local121577074_0001_m_000001_0
     * 2019-10-10 19:57:32,717 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local121577074_0001_m_000002_0
     * 2019-10-10 19:57:32,718 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 19:57:32,718 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 19:57:32,719 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 19:57:32,720 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Default/f3:0+35
     * 2019-10-10 19:57:32,774 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 19:57:32,774 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 19:57:32,774 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 19:57:32,774 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 19:57:32,774 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 19:57:32,775 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 19:57:32,776 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 19:57:32,776 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 19:57:32,776 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 19:57:32,776 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 67; bufvoid = 104857600
     * 2019-10-10 19:57:32,776 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214368(104857472); length = 29/6553600
     * 2019-10-10 19:57:32,778 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 19:57:32,781 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local121577074_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 19:57:32,783 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 19:57:32,783 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local121577074_0001_m_000002_0' done.
     * 2019-10-10 19:57:32,783 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local121577074_0001_m_000002_0
     * 2019-10-10 19:57:32,784 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 19:57:32,787 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 19:57:32,787 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local121577074_0001_r_000000_0
     * 2019-10-10 19:57:32,794 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 19:57:32,795 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 19:57:32,795 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 19:57:32,797 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@3b8c6a54
     * 2019-10-10 19:57:32,807 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 19:57:32,809 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local121577074_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 19:57:32,852 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local121577074_0001_m_000000_0 decomp: 79 len: 83 to MEMORY
     * 2019-10-10 19:57:32,867 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 79 bytes from map-output for attempt_local121577074_0001_m_000000_0
     * 2019-10-10 19:57:32,869 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 79, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->79
     * 2019-10-10 19:57:32,871 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local121577074_0001_m_000002_0 decomp: 55 len: 59 to MEMORY
     * 2019-10-10 19:57:32,872 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 55 bytes from map-output for attempt_local121577074_0001_m_000002_0
     * 2019-10-10 19:57:32,872 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 55, inMemoryMapOutputs.size() -> 2, commitMemory -> 79, usedMemory ->134
     * 2019-10-10 19:57:32,874 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local121577074_0001_m_000001_0 decomp: 37 len: 41 to MEMORY
     * 2019-10-10 19:57:32,874 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 37 bytes from map-output for attempt_local121577074_0001_m_000001_0
     * 2019-10-10 19:57:32,874 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 37, inMemoryMapOutputs.size() -> 3, commitMemory -> 134, usedMemory ->171
     * 2019-10-10 19:57:32,874 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 19:57:32,875 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 19:57:32,875 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 19:57:32,895 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 19:57:32,896 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 150 bytes
     * 2019-10-10 19:57:32,897 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 171 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 19:57:32,898 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 171 bytes from disk
     * 2019-10-10 19:57:32,899 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 19:57:32,899 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 19:57:32,899 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 160 bytes
     * 2019-10-10 19:57:32,900 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 19:57:32,914 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 19:57:32,921 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local121577074_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 19:57:32,922 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 19:57:32,922 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local121577074_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 19:57:32,923 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local121577074_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Default/_temporary/0/task_local121577074_0001_r_000000
     * 2019-10-10 19:57:32,923 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 19:57:32,924 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local121577074_0001_r_000000_0' done.
     * 2019-10-10 19:57:32,924 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local121577074_0001_r_000000_0
     * 2019-10-10 19:57:32,924 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local121577074_0001_r_000001_0
     * 2019-10-10 19:57:32,926 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 19:57:32,927 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 19:57:32,927 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 19:57:32,927 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@5d9141cd
     * 2019-10-10 19:57:32,930 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 19:57:32,931 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local121577074_0001_r_000001_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 19:57:32,933 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local121577074_0001_m_000000_0 decomp: 62 len: 66 to MEMORY
     * 2019-10-10 19:57:32,934 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 62 bytes from map-output for attempt_local121577074_0001_m_000000_0
     * 2019-10-10 19:57:32,934 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 62, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->62
     * 2019-10-10 19:57:32,935 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local121577074_0001_m_000002_0 decomp: 32 len: 36 to MEMORY
     * 2019-10-10 19:57:32,936 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 32 bytes from map-output for attempt_local121577074_0001_m_000002_0
     * 2019-10-10 19:57:32,936 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 32, inMemoryMapOutputs.size() -> 2, commitMemory -> 62, usedMemory ->94
     * 2019-10-10 19:57:32,938 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local121577074_0001_m_000001_0 decomp: 93 len: 97 to MEMORY
     * 2019-10-10 19:57:32,938 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 93 bytes from map-output for attempt_local121577074_0001_m_000001_0
     * 2019-10-10 19:57:32,941 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 93, inMemoryMapOutputs.size() -> 3, commitMemory -> 94, usedMemory ->187
     * 2019-10-10 19:57:32,941 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 19:57:32,942 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 19:57:32,942 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 19:57:32,943 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 19:57:32,943 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 169 bytes
     * 2019-10-10 19:57:32,945 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 187 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 19:57:32,946 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 187 bytes from disk
     * 2019-10-10 19:57:32,946 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 19:57:32,946 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 19:57:32,946 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 177 bytes
     * 2019-10-10 19:57:32,947 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 19:57:32,960 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local121577074_0001_r_000001_0 is done. And is in the process of committing
     * 2019-10-10 19:57:32,962 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 19:57:32,962 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local121577074_0001_r_000001_0 is allowed to commit now
     * 2019-10-10 19:57:32,963 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local121577074_0001_r_000001_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Default/_temporary/0/task_local121577074_0001_r_000001
     * 2019-10-10 19:57:32,964 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 19:57:32,964 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local121577074_0001_r_000001_0' done.
     * 2019-10-10 19:57:32,965 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local121577074_0001_r_000001_0
     * 2019-10-10 19:57:32,965 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 19:57:33,410 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local121577074_0001 running in uber mode : false
     * 2019-10-10 19:57:33,412 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 19:57:33,413 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local121577074_0001 completed successfully
     * 2019-10-10 19:57:33,427 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=7768
     * 		FILE: Number of bytes written=1385404
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=13
     * 		Map output records=33
     * 		Map output bytes=280
     * 		Map output materialized bytes=382
     * 		Input split bytes=384
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=6
     * 		Reduce shuffle bytes=382
     * 		Reduce input records=33
     * 		Reduce output records=6
     * 		Spilled Records=66
     * 		Shuffled Maps =6
     * 		Failed Shuffles=0
     * 		Merged Map outputs=6
     * 		GC time elapsed (ms)=12
     * 		Total committed heap usage (bytes)=1909456896
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
     * 		Bytes Written=63
     *
     * Process finished with exit code 0
     */

}
