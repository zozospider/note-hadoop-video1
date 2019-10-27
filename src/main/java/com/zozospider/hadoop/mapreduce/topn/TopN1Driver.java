package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 输出 Top8 (按 (field1 + field2) 倒叙排列)
 */
public class TopN1Driver {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/TopN1/
     * total 8
     * -rw-r--r--  1 zoz  staff  99 10 27 16:56 f1
     * spiderxmac:input zoz$ cat TopN1/f1
     * abc 10 15
     * qq 30 10
     * xpp 50 20
     * book 100 3
     * good 99 1
     * love 15 10
     * ss 1 99
     * zoo 40 25
     * zoo 100 0
     * what 1 80
     * spiderxmac:input zoz$
     * <p>
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/TopN1/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/TopN1/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/TopN1", "/Users/zoz/zz/other/tmp/MapReduce/output/TopN1"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(TopN1Driver.class);
        job.setMapperClass(TopN1Mapper.class);
        job.setReducerClass(TopN1Reducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(TopN1KeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TopN1KeyWritable.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/TopN1/
     * total 8
     * -rw-r--r--  1 zoz  staff    0 10 27 17:12 _SUCCESS
     * -rw-r--r--  1 zoz  staff  448 10 27 17:12 part-r-00000
     * spiderxmac:output zoz$ cat TopN1/part-r-00000
     * TopN1KeyWritable{field1=1, field2=99, fieldSum=198}	ss
     * TopN1KeyWritable{field1=1, field2=80, fieldSum=160}	what
     * TopN1KeyWritable{field1=40, field2=25, fieldSum=50}	zoo
     * TopN1KeyWritable{field1=50, field2=20, fieldSum=40}	xpp
     * TopN1KeyWritable{field1=10, field2=15, fieldSum=30}	abc
     * TopN1KeyWritable{field1=15, field2=10, fieldSum=20}	love
     * TopN1KeyWritable{field1=30, field2=10, fieldSum=20}	qq
     * TopN1KeyWritable{field1=100, field2=3, fieldSum=6}	book
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-27 17:12:46,454 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-27 17:12:46,559 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-27 17:12:46,559 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-27 17:12:46,774 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-27 17:12:46,789 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-27 17:12:46,805 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-27 17:12:46,858 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-27 17:12:46,938 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local260478739_0001
     * 2019-10-27 17:12:47,104 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-27 17:12:47,106 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-27 17:12:47,106 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local260478739_0001
     * 2019-10-27 17:12:47,107 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-27 17:12:47,109 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-27 17:12:47,132 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-27 17:12:47,133 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local260478739_0001_m_000000_0
     * 2019-10-27 17:12:47,149 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-27 17:12:47,152 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-27 17:12:47,153 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-27 17:12:47,156 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/TopN1/f1:0+99
     * 2019-10-27 17:12:47,207 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-27 17:12:47,207 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-27 17:12:47,207 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-27 17:12:47,207 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-27 17:12:47,207 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-27 17:12:47,210 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-27 17:12:47,214 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-27 17:12:47,214 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-27 17:12:47,214 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-27 17:12:47,214 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 162; bufvoid = 104857600
     * 2019-10-27 17:12:47,214 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214360(104857440); length = 37/6553600
     * 2019-10-27 17:12:47,218 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-27 17:12:47,221 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local260478739_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-27 17:12:47,226 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-27 17:12:47,226 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local260478739_0001_m_000000_0' done.
     * 2019-10-27 17:12:47,226 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local260478739_0001_m_000000_0
     * 2019-10-27 17:12:47,226 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-27 17:12:47,228 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-27 17:12:47,228 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local260478739_0001_r_000000_0
     * 2019-10-27 17:12:47,232 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-27 17:12:47,232 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-27 17:12:47,232 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-27 17:12:47,234 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@3a2b38a0
     * 2019-10-27 17:12:47,242 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-27 17:12:47,243 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local260478739_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-27 17:12:47,272 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local260478739_0001_m_000000_0 decomp: 184 len: 188 to MEMORY
     * 2019-10-27 17:12:47,281 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 184 bytes from map-output for attempt_local260478739_0001_m_000000_0
     * 2019-10-27 17:12:47,282 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 184, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->184
     * 2019-10-27 17:12:47,283 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-27 17:12:47,284 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-27 17:12:47,284 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-27 17:12:47,288 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-27 17:12:47,288 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 170 bytes
     * 2019-10-27 17:12:47,289 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 184 bytes to disk to satisfy reduce memory limit
     * 2019-10-27 17:12:47,289 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 188 bytes from disk
     * 2019-10-27 17:12:47,289 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-27 17:12:47,289 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-27 17:12:47,290 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 170 bytes
     * 2019-10-27 17:12:47,291 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-27 17:12:47,301 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-27 17:12:47,305 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local260478739_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-27 17:12:47,306 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-27 17:12:47,306 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local260478739_0001_r_000000_0 is allowed to commit now
     * 2019-10-27 17:12:47,306 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local260478739_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/TopN1/_temporary/0/task_local260478739_0001_r_000000
     * 2019-10-27 17:12:47,307 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-27 17:12:47,307 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local260478739_0001_r_000000_0' done.
     * 2019-10-27 17:12:47,307 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local260478739_0001_r_000000_0
     * 2019-10-27 17:12:47,307 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-27 17:12:48,112 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local260478739_0001 running in uber mode : false
     * 2019-10-27 17:12:48,113 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-27 17:12:48,114 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local260478739_0001 completed successfully
     * 2019-10-27 17:12:48,121 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=950
     * 		FILE: Number of bytes written=550504
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=10
     * 		Map output records=10
     * 		Map output bytes=162
     * 		Map output materialized bytes=188
     * 		Input split bytes=118
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=9
     * 		Reduce shuffle bytes=188
     * 		Reduce input records=10
     * 		Reduce output records=8
     * 		Spilled Records=20
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=8
     * 		Total committed heap usage (bytes)=514850816
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=99
     * 	File Output Format Counters
     * 		Bytes Written=460
     *
     * Process finished with exit code 0
     */

}
