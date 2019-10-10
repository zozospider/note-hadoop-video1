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
 * MapReduce 驱动 (非标准方式)
 */
public class CustomDriver2 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/partition/Custom2/
     * total 24
     * -rw-r--r--  1 user  staff    54B 10 10 20:35 f1
     * -rw-r--r--  1 user  staff    59B 10 10 20:35 f2
     * -rw-r--r--  1 user  staff    35B 10 10 20:35 f3
     * ➜  input cat partition/Custom2/f1
     * abc abc love
     * qq
     * love love qq
     * qq love
     * google book book
     * ➜  input cat partition/Custom2/f2
     * love love
     * qq google google
     * abc abc qq book
     * book book
     * do do
     * ➜  input cat partition/Custom2/f3
     * love qq
     * google abc
     * book book do do
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom2/
     * ls: /Users/user/other/tmp/MapReduce/output/partition/Custom2/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/partition/Custom2", "/Users/user/other/tmp/MapReduce/output/partition/Custom2"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Partitioner 为 CustomPartitioner
        job.setPartitionerClass(CustomPartitioner.class);
        // 设置 ReduceTasks 个数为 1 (非标准方式: 等于 1)
        job.setNumReduceTasks(1);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CustomDriver2.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom2/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 20:39 _SUCCESS
     * -rw-r--r--  1 user  staff    39B 10 10 20:39 part-r-00000
     * ➜  output cat partition/Custom2/part-r-00000
     * abc	5
     * book	7
     * do	4
     * google	4
     * love	7
     * qq	6
     * ➜  output
     */

    /**
     * 2019-10-10 20:39:11,484 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 20:39:11,779 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 20:39:11,780 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 20:39:12,401 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 20:39:12,406 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 20:39:12,435 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 20:39:12,484 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 20:39:12,594 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1812358391_0001
     * 2019-10-10 20:39:12,803 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 20:39:12,805 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1812358391_0001
     * 2019-10-10 20:39:12,806 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 20:39:12,812 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:39:12,814 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 20:39:12,861 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 20:39:12,861 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1812358391_0001_m_000000_0
     * 2019-10-10 20:39:12,890 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:39:12,898 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:39:12,899 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:39:12,905 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom2/f2:0+59
     * 2019-10-10 20:39:12,978 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:39:12,978 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:39:12,978 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:39:12,978 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:39:12,978 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:39:12,982 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:39:12,992 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:39:12,992 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:39:12,992 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:39:12,992 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 111; bufvoid = 104857600
     * 2019-10-10 20:39:12,992 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214348(104857392); length = 49/6553600
     * 2019-10-10 20:39:13,003 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:39:13,008 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1812358391_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 20:39:13,019 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:39:13,019 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1812358391_0001_m_000000_0' done.
     * 2019-10-10 20:39:13,020 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1812358391_0001_m_000000_0
     * 2019-10-10 20:39:13,023 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1812358391_0001_m_000001_0
     * 2019-10-10 20:39:13,025 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:39:13,026 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:39:13,026 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:39:13,028 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom2/f1:0+54
     * 2019-10-10 20:39:13,102 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:39:13,102 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:39:13,102 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:39:13,102 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:39:13,102 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:39:13,103 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:39:13,105 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:39:13,105 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:39:13,105 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:39:13,106 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 102; bufvoid = 104857600
     * 2019-10-10 20:39:13,106 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214352(104857408); length = 45/6553600
     * 2019-10-10 20:39:13,109 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:39:13,112 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1812358391_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 20:39:13,115 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:39:13,115 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1812358391_0001_m_000001_0' done.
     * 2019-10-10 20:39:13,115 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1812358391_0001_m_000001_0
     * 2019-10-10 20:39:13,116 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1812358391_0001_m_000002_0
     * 2019-10-10 20:39:13,118 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:39:13,119 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:39:13,119 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:39:13,120 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom2/f3:0+35
     * 2019-10-10 20:39:13,186 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:39:13,187 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:39:13,187 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:39:13,187 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:39:13,187 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:39:13,187 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:39:13,189 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:39:13,189 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:39:13,189 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:39:13,189 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 67; bufvoid = 104857600
     * 2019-10-10 20:39:13,190 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214368(104857472); length = 29/6553600
     * 2019-10-10 20:39:13,192 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:39:13,195 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1812358391_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 20:39:13,197 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:39:13,197 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1812358391_0001_m_000002_0' done.
     * 2019-10-10 20:39:13,197 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1812358391_0001_m_000002_0
     * 2019-10-10 20:39:13,198 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 20:39:13,200 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 20:39:13,201 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1812358391_0001_r_000000_0
     * 2019-10-10 20:39:13,209 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:39:13,210 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:39:13,210 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:39:13,213 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@1ee11682
     * 2019-10-10 20:39:13,230 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:39:13,233 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1812358391_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:39:13,281 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1812358391_0001_m_000002_0 decomp: 85 len: 89 to MEMORY
     * 2019-10-10 20:39:13,294 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 85 bytes from map-output for attempt_local1812358391_0001_m_000002_0
     * 2019-10-10 20:39:13,296 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 85, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->85
     * 2019-10-10 20:39:13,299 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1812358391_0001_m_000001_0 decomp: 128 len: 132 to MEMORY
     * 2019-10-10 20:39:13,299 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 128 bytes from map-output for attempt_local1812358391_0001_m_000001_0
     * 2019-10-10 20:39:13,300 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 128, inMemoryMapOutputs.size() -> 2, commitMemory -> 85, usedMemory ->213
     * 2019-10-10 20:39:13,301 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1812358391_0001_m_000000_0 decomp: 139 len: 143 to MEMORY
     * 2019-10-10 20:39:13,302 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 139 bytes from map-output for attempt_local1812358391_0001_m_000000_0
     * 2019-10-10 20:39:13,302 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 139, inMemoryMapOutputs.size() -> 3, commitMemory -> 213, usedMemory ->352
     * 2019-10-10 20:39:13,303 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:39:13,303 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:39:13,304 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:39:13,322 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:39:13,322 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 334 bytes
     * 2019-10-10 20:39:13,326 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 352 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:39:13,327 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 352 bytes from disk
     * 2019-10-10 20:39:13,327 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:39:13,327 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:39:13,328 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 342 bytes
     * 2019-10-10 20:39:13,329 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:39:13,353 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 20:39:13,362 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1812358391_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 20:39:13,363 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:39:13,363 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1812358391_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 20:39:13,364 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1812358391_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom2/_temporary/0/task_local1812358391_0001_r_000000
     * 2019-10-10 20:39:13,365 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:39:13,365 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1812358391_0001_r_000000_0' done.
     * 2019-10-10 20:39:13,365 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1812358391_0001_r_000000_0
     * 2019-10-10 20:39:13,365 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 20:39:13,810 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1812358391_0001 running in uber mode : false
     * 2019-10-10 20:39:13,811 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 20:39:13,812 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1812358391_0001 completed successfully
     * 2019-10-10 20:39:13,822 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=5163
     * 		FILE: Number of bytes written=1115673
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=13
     * 		Map output records=33
     * 		Map output bytes=280
     * 		Map output materialized bytes=364
     * 		Input split bytes=384
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=6
     * 		Reduce shuffle bytes=364
     * 		Reduce input records=33
     * 		Reduce output records=6
     * 		Spilled Records=66
     * 		Shuffled Maps =3
     * 		Failed Shuffles=0
     * 		Merged Map outputs=3
     * 		GC time elapsed (ms)=11
     * 		Total committed heap usage (bytes)=1464336384
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
     * 		Bytes Written=51
     *
     * Process finished with exit code 0
     */

}
