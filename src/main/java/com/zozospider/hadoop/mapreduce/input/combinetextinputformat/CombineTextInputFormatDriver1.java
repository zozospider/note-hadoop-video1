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
public class CombineTextInputFormatDriver1 {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/combinetextinputformat1/
     * total 3024
     * -rw-r--r--  1 user  staff    79B 10 10 13:53 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:53 b
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat1/
     * ls: /Users/user/other/tmp/mapReduce/output/combinetextinputformat1/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/combinetextinputformat1", "/Users/user/other/tmp/mapReduce/output/combinetextinputformat1"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextInputFormatDriver1.class);
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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat1/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 14:08 _SUCCESS
     * -rw-r--r--  1 user  staff   123B 10 10 14:08 part-r-00000
     * ➜  output cat combinetextinputformat1/part-r-00000
     * abc	39012
     * awesome	19505
     * book	39010
     * enough	19505
     * google	19505
     * love	58516
     * me	19505
     * please	19505
     * qq	39011
     * see	19505
     * who	19505
     * ➜  output
     */

    /**
     * 2019-10-10 14:08:22,413 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 14:08:22,896 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 14:08:22,898 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 14:08:23,557 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 14:08:23,562 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 14:08:23,584 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 2
     * 2019-10-10 14:08:23,604 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 1540911
     * 2019-10-10 14:08:23,644 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-10 14:08:23,769 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local152096783_0001
     * 2019-10-10 14:08:24,015 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 14:08:24,016 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 14:08:24,016 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local152096783_0001
     * 2019-10-10 14:08:24,022 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:08:24,024 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 14:08:24,075 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 14:08:24,076 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local152096783_0001_m_000000_0
     * 2019-10-10 14:08:24,107 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:08:24,115 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:08:24,115 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:08:24,121 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat1/a:0+79,/Users/user/other/tmp/mapReduce/input/combinetextinputformat1/b:0+1540832
     * 2019-10-10 14:08:24,199 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:08:24,199 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:08:24,199 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:08:24,199 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:08:24,199 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:08:24,201 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:08:24,579 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:08:24,579 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:08:24,579 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:08:24,579 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 2789247; bufvoid = 104857600
     * 2019-10-10 14:08:24,579 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 24966064(99864256); length = 1248333/6553600
     * 2019-10-10 14:08:24,838 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:08:24,843 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local152096783_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:08:24,852 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:08:24,852 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local152096783_0001_m_000000_0' done.
     * 2019-10-10 14:08:24,852 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local152096783_0001_m_000000_0
     * 2019-10-10 14:08:24,852 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 14:08:24,855 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 14:08:24,855 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local152096783_0001_r_000000_0
     * 2019-10-10 14:08:24,860 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:08:24,861 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:08:24,861 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:08:24,863 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6e5926f4
     * 2019-10-10 14:08:24,882 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 14:08:24,887 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local152096783_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 14:08:24,936 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local152096783_0001_m_000000_0 decomp: 3413417 len: 3413421 to MEMORY
     * 2019-10-10 14:08:24,956 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 3413417 bytes from map-output for attempt_local152096783_0001_m_000000_0
     * 2019-10-10 14:08:24,958 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 3413417, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->3413417
     * 2019-10-10 14:08:24,960 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 14:08:24,960 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 14:08:24,961 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 14:08:24,968 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 14:08:24,968 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 3413411 bytes
     * 2019-10-10 14:08:25,018 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local152096783_0001 running in uber mode : false
     * 2019-10-10 14:08:25,020 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 14:08:25,200 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 3413417 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 14:08:25,200 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 3413421 bytes from disk
     * 2019-10-10 14:08:25,201 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 14:08:25,201 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 14:08:25,204 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 3413411 bytes
     * 2019-10-10 14:08:25,205 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 14:08:25,220 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 14:08:25,504 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local152096783_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:08:25,506 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 14:08:25,506 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local152096783_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 14:08:25,508 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local152096783_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/combinetextinputformat1/_temporary/0/task_local152096783_0001_r_000000
     * 2019-10-10 14:08:25,509 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 14:08:25,509 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local152096783_0001_r_000000_0' done.
     * 2019-10-10 14:08:25,509 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local152096783_0001_r_000000_0
     * 2019-10-10 14:08:25,509 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 14:08:26,025 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 14:08:26,026 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local152096783_0001 completed successfully
     * 2019-10-10 14:08:26,039 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=9909280
     * 		FILE: Number of bytes written=10794314
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=117032
     * 		Map output records=312084
     * 		Map output bytes=2789247
     * 		Map output materialized bytes=3413421
     * 		Input split bytes=245
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=11
     * 		Reduce shuffle bytes=3413421
     * 		Reduce input records=312084
     * 		Reduce output records=11
     * 		Spilled Records=624168
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=22
     * 		Total committed heap usage (bytes)=540016640
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
     * 		Bytes Written=135
     *
     * Process finished with exit code 0
     */

}
