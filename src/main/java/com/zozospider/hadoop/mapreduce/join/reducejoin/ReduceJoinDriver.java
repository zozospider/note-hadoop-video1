package com.zozospider.hadoop.mapreduce.join.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 在 fa 的每 1 行尾部添加 field3 (bId) 对应的 fb 中的 field2 (bName).
 */
public class ReduceJoinDriver {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/join/ReduceJoin/
     * total 16
     * -rw-r--r--  1 zoz  staff  105 10 14 23:20 fa
     * -rw-r--r--  1 zoz  staff   67 10 15 00:42 fb
     * spiderxmac:input zoz$ cat join/ReduceJoin/fa
     * 1 Frank 3
     * 2 Jack 5
     * 3 John 6
     * 4 Olivia 2
     * 5 Ava 5
     * 6 Mia 1
     * 7 David 3
     * 8 Anna 4
     * 9 Lily 2
     * 10 Luke 1
     * 11 Oliver 2
     * spiderxmac:input zoz$ cat join/ReduceJoin/fb
     * 1 New_York
     * 2 Los_Angeles
     * 3 Chicago
     * 4 Houston
     * 5 Dallas
     * 6 Washington
     * spiderxmac:input zoz$
     * <p>
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/join/ReduceJoin/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/join/ReduceJoin/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/join/ReduceJoin", "/Users/zoz/zz/other/tmp/MapReduce/output/join/ReduceJoin"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(ReduceJoinDriver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ReduceJoinValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }


    /**
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/join/ReduceJoin/
     * total 8
     * -rw-r--r--  1 zoz  staff    0 10 15 00:43 _SUCCESS
     * -rw-r--r--  1 zoz  staff  505 10 15 00:43 part-r-00000
     * spiderxmac:output zoz$ cat join/ReduceJoin/part-r-00000
     * aId: 10, aName: Luke, bId: 1, bName: New_York
     * aId: 6, aName: Mia, bId: 1, bName: New_York
     * aId: 11, aName: Oliver, bId: 2, bName: Los_Angeles
     * aId: 9, aName: Lily, bId: 2, bName: Los_Angeles
     * aId: 4, aName: Olivia, bId: 2, bName: Los_Angeles
     * aId: 7, aName: David, bId: 3, bName: Chicago
     * aId: 1, aName: Frank, bId: 3, bName: Chicago
     * aId: 8, aName: Anna, bId: 4, bName: Houston
     * aId: 5, aName: Ava, bId: 5, bName: Dallas
     * aId: 2, aName: Jack, bId: 5, bName: Dallas
     * aId: 3, aName: John, bId: 6, bName: Washington
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-15 00:43:25,298 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-15 00:43:25,459 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-15 00:43:25,459 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-15 00:43:25,875 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-15 00:43:25,880 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-15 00:43:25,902 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 2
     * 2019-10-15 00:43:25,944 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:2
     * 2019-10-15 00:43:26,016 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1385696579_0001
     * 2019-10-15 00:43:26,130 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-15 00:43:26,130 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-15 00:43:26,131 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1385696579_0001
     * 2019-10-15 00:43:26,132 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 00:43:26,133 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-15 00:43:26,157 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-15 00:43:26,157 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1385696579_0001_m_000000_0
     * 2019-10-15 00:43:26,174 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 00:43:26,178 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-15 00:43:26,179 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-15 00:43:26,182 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/join/ReduceJoin/fa:0+105
     * 2019-10-15 00:43:26,235 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-15 00:43:26,235 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-15 00:43:26,235 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-15 00:43:26,235 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-15 00:43:26,235 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-15 00:43:26,236 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-15 00:43:26,241 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-15 00:43:26,241 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-15 00:43:26,241 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-15 00:43:26,241 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 268; bufvoid = 104857600
     * 2019-10-15 00:43:26,241 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214356(104857424); length = 41/6553600
     * 2019-10-15 00:43:26,245 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-15 00:43:26,247 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1385696579_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-15 00:43:26,252 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-15 00:43:26,252 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1385696579_0001_m_000000_0' done.
     * 2019-10-15 00:43:26,252 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1385696579_0001_m_000000_0
     * 2019-10-15 00:43:26,252 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1385696579_0001_m_000001_0
     * 2019-10-15 00:43:26,253 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 00:43:26,253 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-15 00:43:26,253 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-15 00:43:26,254 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/join/ReduceJoin/fb:0+67
     * 2019-10-15 00:43:26,299 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-15 00:43:26,299 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-15 00:43:26,299 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-15 00:43:26,299 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-15 00:43:26,299 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-15 00:43:26,300 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-15 00:43:26,301 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-15 00:43:26,301 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-15 00:43:26,301 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-15 00:43:26,301 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 169; bufvoid = 104857600
     * 2019-10-15 00:43:26,301 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214376(104857504); length = 21/6553600
     * 2019-10-15 00:43:26,302 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-15 00:43:26,303 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1385696579_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-15 00:43:26,304 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-15 00:43:26,304 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1385696579_0001_m_000001_0' done.
     * 2019-10-15 00:43:26,304 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1385696579_0001_m_000001_0
     * 2019-10-15 00:43:26,305 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-15 00:43:26,306 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-15 00:43:26,306 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1385696579_0001_r_000000_0
     * 2019-10-15 00:43:26,311 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-15 00:43:26,311 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-15 00:43:26,311 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-15 00:43:26,313 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@5bc3940e
     * 2019-10-15 00:43:26,321 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-15 00:43:26,323 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1385696579_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-15 00:43:26,351 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1385696579_0001_m_000000_0 decomp: 292 len: 296 to MEMORY
     * 2019-10-15 00:43:26,362 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 292 bytes from map-output for attempt_local1385696579_0001_m_000000_0
     * 2019-10-15 00:43:26,362 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 292, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->292
     * 2019-10-15 00:43:26,364 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1385696579_0001_m_000001_0 decomp: 183 len: 187 to MEMORY
     * 2019-10-15 00:43:26,364 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 183 bytes from map-output for attempt_local1385696579_0001_m_000001_0
     * 2019-10-15 00:43:26,364 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 183, inMemoryMapOutputs.size() -> 2, commitMemory -> 292, usedMemory ->475
     * 2019-10-15 00:43:26,364 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-15 00:43:26,365 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-15 00:43:26,365 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 2 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-15 00:43:26,369 INFO [org.apache.hadoop.mapred.Merger] - Merging 2 sorted segments
     * 2019-10-15 00:43:26,369 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 2 segments left of total size: 463 bytes
     * 2019-10-15 00:43:26,370 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 2 segments, 475 bytes to disk to satisfy reduce memory limit
     * 2019-10-15 00:43:26,370 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 477 bytes from disk
     * 2019-10-15 00:43:26,370 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-15 00:43:26,371 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-15 00:43:26,372 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 467 bytes
     * 2019-10-15 00:43:26,372 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-15 00:43:26,391 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-15 00:43:26,418 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1385696579_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-15 00:43:26,419 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 2 / 2 copied.
     * 2019-10-15 00:43:26,419 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1385696579_0001_r_000000_0 is allowed to commit now
     * 2019-10-15 00:43:26,419 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1385696579_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/join/ReduceJoin/_temporary/0/task_local1385696579_0001_r_000000
     * 2019-10-15 00:43:26,420 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-15 00:43:26,420 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1385696579_0001_r_000000_0' done.
     * 2019-10-15 00:43:26,420 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1385696579_0001_r_000000_0
     * 2019-10-15 00:43:26,420 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-15 00:43:27,135 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1385696579_0001 running in uber mode : false
     * 2019-10-15 00:43:27,136 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-15 00:43:27,136 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1385696579_0001 completed successfully
     * 2019-10-15 00:43:27,146 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=2995
     * 		FILE: Number of bytes written=831832
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=17
     * 		Map output records=17
     * 		Map output bytes=437
     * 		Map output materialized bytes=483
     * 		Input split bytes=256
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=6
     * 		Reduce shuffle bytes=483
     * 		Reduce input records=17
     * 		Reduce output records=11
     * 		Spilled Records=34
     * 		Shuffled Maps =2
     * 		Failed Shuffles=0
     * 		Merged Map outputs=2
     * 		GC time elapsed (ms)=7
     * 		Total committed heap usage (bytes)=983040000
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=172
     * 	File Output Format Counters
     * 		Bytes Written=517
     *
     * Process finished with exit code 0
     */

}
