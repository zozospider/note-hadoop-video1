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
public class CombineTextDriver2 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/CombineText2/
     * total 13520
     * -rw-r--r--  1 user  staff    79B 10 10 13:56 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:46 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:46 c
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText2/
     * ls: /Users/user/other/tmp/MapReduce/output/CombineText2/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/CombineText2", "/Users/user/other/tmp/MapReduce/output/CombineText2"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextDriver2.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/CombineText2/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 16:17 _SUCCESS
     * -rw-r--r--  1 user  staff   131B 10 10 16:17 part-r-00000
     * ➜  output cat CombineText2/part-r-00000
     * 	65
     * abc	174970
     * awesome	87470
     * book	174951
     * enough	87481
     * google	87470
     * love	262436
     * me	87481
     * please	87470
     * qq	175007
     * see	87470
     * who	87470
     * ➜  output
     */

    /**
     * 2019-10-10 16:17:33,275 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 16:17:33,560 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 16:17:33,561 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 16:17:34,056 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 16:17:34,067 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 16:17:34,115 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 16:17:34,145 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 2684950
     * 2019-10-10 16:17:34,207 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:2
     * 2019-10-10 16:17:34,320 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1998002193_0001
     * 2019-10-10 16:17:34,628 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 16:17:34,636 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:34,639 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 16:17:34,639 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 16:17:34,640 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1998002193_0001
     * 2019-10-10 16:17:34,690 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 16:17:34,690 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1998002193_0001_m_000000_0
     * 2019-10-10 16:17:34,720 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:34,726 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:17:34,726 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:17:34,731 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText2/a:0+79,/Users/user/other/tmp/MapReduce/input/CombineText2/b:0+1540832,/Users/user/other/tmp/MapReduce/input/CombineText2/c:0+2684950
     * 2019-10-10 16:17:34,789 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:17:34,789 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:17:34,789 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:17:34,789 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:17:34,789 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:17:34,791 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:17:35,399 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:17:35,399 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:17:35,399 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:17:35,399 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649362; bufvoid = 104857600
     * 2019-10-10 16:17:35,399 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790904(91163616); length = 3423493/6553600
     * 2019-10-10 16:17:35,653 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1998002193_0001 running in uber mode : false
     * 2019-10-10 16:17:35,655 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 16:17:35,968 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:17:35,982 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1998002193_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:17:35,991 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:17:35,991 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1998002193_0001_m_000000_0' done.
     * 2019-10-10 16:17:35,991 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1998002193_0001_m_000000_0
     * 2019-10-10 16:17:35,991 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1998002193_0001_m_000001_0
     * 2019-10-10 16:17:35,992 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:35,993 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:17:35,993 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:17:35,994 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/MapReduce/input/CombineText2/c:2684950+2684950
     * 2019-10-10 16:17:36,110 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:17:36,110 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:17:36,110 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:17:36,110 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:17:36,110 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:17:36,112 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:17:36,583 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:17:36,583 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:17:36,583 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:17:36,583 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 4860413; bufvoid = 104857600
     * 2019-10-10 16:17:36,583 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 24038932(96155728); length = 2175465/6553600
     * 2019-10-10 16:17:36,662 INFO [org.apache.hadoop.mapreduce.Job] -  map 50% reduce 0%
     * 2019-10-10 16:17:36,831 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:17:36,832 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1998002193_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 16:17:36,834 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:17:36,834 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1998002193_0001_m_000001_0' done.
     * 2019-10-10 16:17:36,834 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1998002193_0001_m_000001_0
     * 2019-10-10 16:17:36,834 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 16:17:36,838 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 16:17:36,838 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1998002193_0001_r_000000_0
     * 2019-10-10 16:17:36,845 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:17:36,846 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:17:36,846 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:17:36,848 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@2e1963a5
     * 2019-10-10 16:17:36,858 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 16:17:36,860 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1998002193_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 16:17:36,895 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1998002193_0001_m_000001_0 decomp: 5948149 len: 5948153 to MEMORY
     * 2019-10-10 16:17:36,907 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 5948149 bytes from map-output for attempt_local1998002193_0001_m_000001_0
     * 2019-10-10 16:17:36,909 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 5948149, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->5948149
     * 2019-10-10 16:17:36,925 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1998002193_0001_m_000000_0 decomp: 9361112 len: 9361116 to MEMORY
     * 2019-10-10 16:17:36,941 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9361112 bytes from map-output for attempt_local1998002193_0001_m_000000_0
     * 2019-10-10 16:17:36,941 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9361112, inMemoryMapOutputs.size() -> 2, commitMemory -> 5948149, usedMemory ->15309261
     * 2019-10-10 16:17:36,943 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 16:17:36,944 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-10 16:17:36,944 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 2 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 16:17:36,951 INFO [org.apache.hadoop.mapred.Merger] - Merging 2 sorted segments
     * 2019-10-10 16:17:36,952 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 2 segments left of total size: 15309252 bytes
     * 2019-10-10 16:17:37,441 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 2 segments, 15309261 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 16:17:37,442 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 15309263 bytes from disk
     * 2019-10-10 16:17:37,442 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 16:17:37,442 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:17:37,443 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 15309256 bytes
     * 2019-10-10 16:17:37,443 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-10 16:17:37,455 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 16:17:37,664 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 16:17:38,207 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1998002193_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:17:38,208 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-10 16:17:38,209 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1998002193_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 16:17:38,209 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1998002193_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/CombineText2/_temporary/0/task_local1998002193_0001_r_000000
     * 2019-10-10 16:17:38,210 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 16:17:38,210 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1998002193_0001_r_000000_0' done.
     * 2019-10-10 16:17:38,210 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1998002193_0001_r_000000_0
     * 2019-10-10 16:17:38,210 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 16:17:38,667 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 16:17:38,667 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1998002193_0001 completed successfully
     * 2019-10-10 16:17:38,679 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=48686908
     * 		FILE: Number of bytes written=56124759
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=524978
     * 		Map output records=1399741
     * 		Map output bytes=12509775
     * 		Map output materialized bytes=15309269
     * 		Input split bytes=446
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=12
     * 		Reduce shuffle bytes=15309269
     * 		Reduce input records=1399741
     * 		Reduce output records=12
     * 		Spilled Records=2799482
     * 		Shuffled Maps =2
     * 		Failed Shuffles=0
     * 		Merged Map outputs=2
     * 		GC time elapsed (ms)=54
     * 		Total committed heap usage (bytes)=1311244288
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
     * 		Bytes Written=143
     *
     * Process finished with exit code 0
     */

}
