package com.zozospider.hadoop.mapreduce.input.keyvaluetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动
 */
public class KeyValueTextInputFormatDriver {

    /**
     * ➜  input ll ll /Users/user/other/tmp/mapReduce/input/keyvaluetextinputformat/
     * ls: ll: No such file or directory
     * /Users/user/other/tmp/mapReduce/input/keyvaluetextinputformat:
     * total 8
     * -rw-r--r--  1 user  staff    79B 10  9 21:23 inputWords
     * ➜  input cat keyvaluetextinputformat/inputWords
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/keyvaluetextinputformat/
     * ls: /Users/user/other/tmp/mapReduce/output/keyvaluetextinputformat/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/keyvaluetextinputformat", "/Users/user/other/tmp/mapReduce/output/keyvaluetextinputformat"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        // 设置 KeyValueTextInputFormat 的 key value 使用空格分割
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(KeyValueTextInputFormatDriver.class);
        job.setMapperClass(KeyValueTextInputFormatMapper.class);
        job.setReducerClass(KeyValueTextInputFormatReducer.class);

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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/keyvaluetextinputformat/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10  9 21:26 _SUCCESS
     * -rw-r--r--  1 user  staff    35B 10  9 21:26 part-r-00000
     * ➜  output cat keyvaluetextinputformat/part-r-00000
     * abc	1
     * enough	1
     * please	1
     * qq	2
     * see	1
     * ➜  output
     */

    /**
     * 2019-10-09 21:26:29,087 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-09 21:26:29,274 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-09 21:26:29,275 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-09 21:26:29,773 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-09 21:26:29,778 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-09 21:26:29,796 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-09 21:26:29,963 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-09 21:26:30,137 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1283814843_0001
     * 2019-10-09 21:26:30,372 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-09 21:26:30,374 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-09 21:26:30,374 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1283814843_0001
     * 2019-10-09 21:26:30,379 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 21:26:30,381 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-09 21:26:30,432 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-09 21:26:30,433 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1283814843_0001_m_000000_0
     * 2019-10-09 21:26:30,465 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 21:26:30,584 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 21:26:30,584 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 21:26:30,588 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/mapReduce/input/keyvaluetextinputformat/inputWords:0+79
     * 2019-10-09 21:26:30,647 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-09 21:26:30,647 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-09 21:26:30,647 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-09 21:26:30,647 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-09 21:26:30,647 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-09 21:26:30,649 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-09 21:26:30,655 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-09 21:26:30,655 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-09 21:26:30,655 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-09 21:26:30,655 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 52; bufvoid = 104857600
     * 2019-10-09 21:26:30,655 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214376(104857504); length = 21/6553600
     * 2019-10-09 21:26:30,661 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-09 21:26:30,664 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1283814843_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-09 21:26:30,671 INFO [org.apache.hadoop.mapred.LocalJobRunner] - file:/Users/user/other/tmp/mapReduce/input/keyvaluetextinputformat/inputWords:0+79
     * 2019-10-09 21:26:30,671 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1283814843_0001_m_000000_0' done.
     * 2019-10-09 21:26:30,671 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1283814843_0001_m_000000_0
     * 2019-10-09 21:26:30,671 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-09 21:26:30,673 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-09 21:26:30,673 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1283814843_0001_r_000000_0
     * 2019-10-09 21:26:30,680 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-09 21:26:30,681 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-09 21:26:30,681 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-09 21:26:30,683 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@5aa5d617
     * 2019-10-09 21:26:30,693 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-09 21:26:30,695 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1283814843_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-09 21:26:30,734 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1283814843_0001_m_000000_0 decomp: 66 len: 70 to MEMORY
     * 2019-10-09 21:26:30,750 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 66 bytes from map-output for attempt_local1283814843_0001_m_000000_0
     * 2019-10-09 21:26:30,752 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 66, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->66
     * 2019-10-09 21:26:30,753 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-09 21:26:30,754 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-09 21:26:30,755 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-09 21:26:30,771 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-09 21:26:30,771 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 60 bytes
     * 2019-10-09 21:26:30,772 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 66 bytes to disk to satisfy reduce memory limit
     * 2019-10-09 21:26:30,772 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 70 bytes from disk
     * 2019-10-09 21:26:30,773 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-09 21:26:30,773 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-09 21:26:30,773 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 60 bytes
     * 2019-10-09 21:26:30,774 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-09 21:26:30,789 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-09 21:26:30,800 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1283814843_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-09 21:26:30,804 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-09 21:26:30,804 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1283814843_0001_r_000000_0 is allowed to commit now
     * 2019-10-09 21:26:30,806 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1283814843_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/keyvaluetextinputformat/_temporary/0/task_local1283814843_0001_r_000000
     * 2019-10-09 21:26:30,807 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-09 21:26:30,807 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1283814843_0001_r_000000_0' done.
     * 2019-10-09 21:26:30,807 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1283814843_0001_r_000000_0
     * 2019-10-09 21:26:30,808 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-09 21:26:31,378 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1283814843_0001 running in uber mode : false
     * 2019-10-09 21:26:31,379 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-09 21:26:31,380 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1283814843_0001 completed successfully
     * 2019-10-09 21:26:31,389 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=722
     * 		FILE: Number of bytes written=557777
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=6
     * 		Map output records=6
     * 		Map output bytes=52
     * 		Map output materialized bytes=70
     * 		Input split bytes=142
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=5
     * 		Reduce shuffle bytes=70
     * 		Reduce input records=6
     * 		Reduce output records=5
     * 		Spilled Records=12
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=10
     * 		Total committed heap usage (bytes)=502792192
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=79
     * 	File Output Format Counters
     * 		Bytes Written=47
     *
     * Process finished with exit code 0
     */

}
