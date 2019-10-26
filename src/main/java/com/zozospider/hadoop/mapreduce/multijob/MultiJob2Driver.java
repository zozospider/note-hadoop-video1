package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 找出每个单词在不同文件中的位置 (当前文件的偏移量)
 */
public class MultiJob2Driver {

    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/MultiJob/one/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 26 17:33 _SUCCESS
     * -rw-r--r--  1 user  staff   105B 10 26 17:33 part-r-00000
     * ➜  output cat MultiJob/one/_SUCCESS
     * ➜  output cat MultiJob/one/part-r-00000
     * abc	f3,12|f3,8|f1,20|f1,8|f1,0|f2,16
     * why	f2,20|f2,12|f2,0|f3,16|f3,4|f1,16
     * zoo	f1,12|f1,4|f3,0|f2,8|f2,4
     * ➜  output
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/MultiJob/two/
     * ls: /Users/user/other/tmp/MapReduce/output/MultiJob/two/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/output/MultiJob/one", "/Users/user/other/tmp/MapReduce/output/MultiJob/two"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(MultiJob2Driver.class);
        job.setMapperClass(MultiJob2Mapper.class);
        job.setReducerClass(MultiJob2Reducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/MultiJob/two/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 26 18:00 _SUCCESS
     * -rw-r--r--  1 user  staff   105B 10 26 18:00 part-r-00000
     * ➜  output cat MultiJob/two/part-r-00000
     * abc:f1	20,8,0
     * abc:f2	16
     * abc:f3	12,8
     * why:f1	16
     * why:f2	0,12,20
     * why:f3	4,16
     * zoo:f1	12,4
     * zoo:f2	4,8
     * zoo:f3	0
     * ➜  output
     */

    /**
     * 2019-10-26 18:00:57,712 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-26 18:00:58,040 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-26 18:00:58,042 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-26 18:00:58,477 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-26 18:00:58,483 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-26 18:00:58,514 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-26 18:00:58,569 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-26 18:00:58,684 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1044314054_0001
     * 2019-10-26 18:00:58,909 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-26 18:00:58,909 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-26 18:00:58,913 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1044314054_0001
     * 2019-10-26 18:00:58,915 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 18:00:58,917 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-26 18:00:58,970 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-26 18:00:58,971 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1044314054_0001_m_000000_0
     * 2019-10-26 18:00:59,000 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 18:00:59,007 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-26 18:00:59,007 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-26 18:00:59,014 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/output/MultiJob/one/part-r-00000:0+105
     * 2019-10-26 18:00:59,080 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-26 18:00:59,080 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-26 18:00:59,080 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-26 18:00:59,080 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-26 18:00:59,080 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-26 18:00:59,082 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-26 18:00:59,100 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-26 18:00:59,100 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-26 18:00:59,100 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-26 18:00:59,100 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 187; bufvoid = 104857600
     * 2019-10-26 18:00:59,100 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214332(104857328); length = 65/6553600
     * 2019-10-26 18:00:59,109 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-26 18:00:59,113 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1044314054_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-26 18:00:59,122 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-26 18:00:59,122 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1044314054_0001_m_000000_0' done.
     * 2019-10-26 18:00:59,122 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1044314054_0001_m_000000_0
     * 2019-10-26 18:00:59,123 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-26 18:00:59,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-26 18:00:59,128 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1044314054_0001_r_000000_0
     * 2019-10-26 18:00:59,134 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 18:00:59,135 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-26 18:00:59,135 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-26 18:00:59,137 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@69805c1b
     * 2019-10-26 18:00:59,148 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-26 18:00:59,151 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1044314054_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-26 18:00:59,191 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1044314054_0001_m_000000_0 decomp: 223 len: 227 to MEMORY
     * 2019-10-26 18:00:59,193 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 223 bytes from map-output for attempt_local1044314054_0001_m_000000_0
     * 2019-10-26 18:00:59,195 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 223, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->223
     * 2019-10-26 18:00:59,196 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-26 18:00:59,197 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-26 18:00:59,197 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-26 18:00:59,203 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-26 18:00:59,203 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 214 bytes
     * 2019-10-26 18:00:59,204 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 223 bytes to disk to satisfy reduce memory limit
     * 2019-10-26 18:00:59,205 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 227 bytes from disk
     * 2019-10-26 18:00:59,206 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-26 18:00:59,206 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-26 18:00:59,207 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 214 bytes
     * 2019-10-26 18:00:59,207 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-26 18:00:59,222 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-26 18:00:59,239 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1044314054_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-26 18:00:59,241 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-26 18:00:59,241 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1044314054_0001_r_000000_0 is allowed to commit now
     * 2019-10-26 18:00:59,242 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1044314054_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/MultiJob/two/_temporary/0/task_local1044314054_0001_r_000000
     * 2019-10-26 18:00:59,243 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-26 18:00:59,243 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1044314054_0001_r_000000_0' done.
     * 2019-10-26 18:00:59,243 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1044314054_0001_r_000000_0
     * 2019-10-26 18:00:59,243 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-26 18:00:59,917 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1044314054_0001 running in uber mode : false
     * 2019-10-26 18:00:59,918 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-26 18:00:59,919 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1044314054_0001 completed successfully
     * 2019-10-26 18:00:59,931 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=1104
     * 		FILE: Number of bytes written=556186
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=3
     * 		Map output records=17
     * 		Map output bytes=187
     * 		Map output materialized bytes=227
     * 		Input split bytes=134
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=9
     * 		Reduce shuffle bytes=227
     * 		Reduce input records=17
     * 		Reduce output records=9
     * 		Spilled Records=34
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=8
     * 		Total committed heap usage (bytes)=468713472
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=121
     * 	File Output Format Counters
     * 		Bytes Written=117
     *
     * Process finished with exit code 0
     */

}
