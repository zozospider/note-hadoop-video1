package com.zozospider.hadoop.mapreduce.input.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动 (将多个文件合并成一个文件)
 */
public class CustomDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/Custom/
     * total 24
     * -rw-r--r--  1 user  staff    59B 10 10 17:26 f1
     * -rw-r--r--  1 user  staff   126B 10 10 17:29 f2
     * -rw-r--r--  1 user  staff    76B 10 10 17:30 f3
     * ➜  input cat Custom/f1
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * ➜  input cat Custom/f2
     * begin2 and find the other one
     * haha I can't do this
     * yoyo 222222  22222222222222
     * love face love love
     * not at all
     * not at all
     * end2
     * ➜  input cat Custom/f3
     * begin3 3333333333333333
     * why do you like coalar bear?
     * show me the money
     * end3
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/Custom/
     * ls: /Users/user/other/tmp/MapReduce/output/Custom: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/Custom", "/Users/user/other/tmp/MapReduce/output/Custom"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CustomInputFormat, Reduce 输出类型为: SequenceFileOutputFormat
        job.setInputFormatClass(CustomInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CustomDriver.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/Custom/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 17:32 _SUCCESS
     * -rw-r--r--  1 user  staff   543B 10 10 17:32 part-r-00000
     * ➜  output cat Custom/part-r-00000
     * SEQorg.apache.hadoop.io.Text"org.apache.hadoop.io.BytesWritableԫ@�Բr��������t54file:/Users/user/other/tmp/MapReduce/input/Custom/f1;abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * �54file:/Users/user/other/tmp/MapReduce/input/Custom/f2~begin2 and find the other one
     * haha I can't do this
     * yoyo 222222  22222222222222
     * love face love love
     * not at all
     * not at all
     * end2
     * �54file:/Users/user/other/tmp/MapReduce/input/Custom/f3Lbegin3 3333333333333333
     * why do you like coalar bear?
     * show me the money
     * end3
     * ➜  output
     */

    /**
     * 2019-10-10 17:32:38,377 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 17:32:38,762 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 17:32:38,763 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 17:32:39,433 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 17:32:39,438 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 17:32:39,463 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 17:32:39,517 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 17:32:39,686 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local711677067_0001
     * 2019-10-10 17:32:39,923 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 17:32:39,925 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 17:32:39,925 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local711677067_0001
     * 2019-10-10 17:32:39,930 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 17:32:39,932 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 17:32:39,993 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 17:32:39,994 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local711677067_0001_m_000000_0
     * 2019-10-10 17:32:40,026 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 17:32:40,040 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 17:32:40,041 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 17:32:40,046 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/Custom/f2:0+126
     * 2019-10-10 17:32:40,101 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 17:32:40,101 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 17:32:40,101 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 17:32:40,101 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 17:32:40,101 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 17:32:40,103 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 17:32:40,108 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 17:32:40,108 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 17:32:40,108 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 17:32:40,108 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 183; bufvoid = 104857600
     * 2019-10-10 17:32:40,108 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214396(104857584); length = 1/6553600
     * 2019-10-10 17:32:40,113 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 17:32:40,116 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local711677067_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 17:32:40,124 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 17:32:40,124 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local711677067_0001_m_000000_0' done.
     * 2019-10-10 17:32:40,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local711677067_0001_m_000000_0
     * 2019-10-10 17:32:40,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local711677067_0001_m_000001_0
     * 2019-10-10 17:32:40,127 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 17:32:40,128 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 17:32:40,128 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 17:32:40,129 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/Custom/f3:0+76
     * 2019-10-10 17:32:40,217 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 17:32:40,218 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 17:32:40,218 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 17:32:40,218 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 17:32:40,218 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 17:32:40,218 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 17:32:40,220 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 17:32:40,220 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 17:32:40,220 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 17:32:40,220 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 133; bufvoid = 104857600
     * 2019-10-10 17:32:40,220 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214396(104857584); length = 1/6553600
     * 2019-10-10 17:32:40,221 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 17:32:40,223 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local711677067_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 17:32:40,226 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 17:32:40,226 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local711677067_0001_m_000001_0' done.
     * 2019-10-10 17:32:40,226 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local711677067_0001_m_000001_0
     * 2019-10-10 17:32:40,226 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local711677067_0001_m_000002_0
     * 2019-10-10 17:32:40,227 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 17:32:40,228 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 17:32:40,228 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 17:32:40,229 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/Custom/f1:0+59
     * 2019-10-10 17:32:40,279 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 17:32:40,279 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 17:32:40,279 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 17:32:40,279 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 17:32:40,279 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 17:32:40,280 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 17:32:40,298 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 17:32:40,298 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 17:32:40,298 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 17:32:40,298 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 116; bufvoid = 104857600
     * 2019-10-10 17:32:40,298 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214396(104857584); length = 1/6553600
     * 2019-10-10 17:32:40,299 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 17:32:40,300 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local711677067_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 17:32:40,301 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 17:32:40,301 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local711677067_0001_m_000002_0' done.
     * 2019-10-10 17:32:40,301 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local711677067_0001_m_000002_0
     * 2019-10-10 17:32:40,302 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 17:32:40,304 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 17:32:40,304 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local711677067_0001_r_000000_0
     * 2019-10-10 17:32:40,309 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 17:32:40,310 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 17:32:40,310 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 17:32:40,312 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@27ab083e
     * 2019-10-10 17:32:40,324 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 17:32:40,326 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local711677067_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 17:32:40,370 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local711677067_0001_m_000000_0 decomp: 188 len: 192 to MEMORY
     * 2019-10-10 17:32:40,385 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 188 bytes from map-output for attempt_local711677067_0001_m_000000_0
     * 2019-10-10 17:32:40,388 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 188, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->188
     * 2019-10-10 17:32:40,390 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local711677067_0001_m_000001_0 decomp: 137 len: 141 to MEMORY
     * 2019-10-10 17:32:40,391 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 137 bytes from map-output for attempt_local711677067_0001_m_000001_0
     * 2019-10-10 17:32:40,391 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 137, inMemoryMapOutputs.size() -> 2, commitMemory -> 188, usedMemory ->325
     * 2019-10-10 17:32:40,393 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local711677067_0001_m_000002_0 decomp: 120 len: 124 to MEMORY
     * 2019-10-10 17:32:40,393 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 120 bytes from map-output for attempt_local711677067_0001_m_000002_0
     * 2019-10-10 17:32:40,393 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 120, inMemoryMapOutputs.size() -> 3, commitMemory -> 325, usedMemory ->445
     * 2019-10-10 17:32:40,394 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 17:32:40,395 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 17:32:40,395 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 17:32:40,404 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 17:32:40,408 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 279 bytes
     * 2019-10-10 17:32:40,410 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 445 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 17:32:40,410 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 445 bytes from disk
     * 2019-10-10 17:32:40,411 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 17:32:40,411 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 17:32:40,411 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 386 bytes
     * 2019-10-10 17:32:40,412 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 17:32:40,462 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 17:32:40,469 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local711677067_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 17:32:40,471 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 17:32:40,471 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local711677067_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 17:32:40,472 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local711677067_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/Custom/_temporary/0/task_local711677067_0001_r_000000
     * 2019-10-10 17:32:40,473 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 17:32:40,473 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local711677067_0001_r_000000_0' done.
     * 2019-10-10 17:32:40,473 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local711677067_0001_r_000000_0
     * 2019-10-10 17:32:40,473 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 17:32:40,930 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local711677067_0001 running in uber mode : false
     * 2019-10-10 17:32:40,931 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 17:32:40,932 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local711677067_0001 completed successfully
     * 2019-10-10 17:32:40,944 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=5426
     * 		FILE: Number of bytes written=1112059
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=3
     * 		Map output records=3
     * 		Map output bytes=432
     * 		Map output materialized bytes=457
     * 		Input split bytes=351
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=3
     * 		Reduce shuffle bytes=457
     * 		Reduce input records=3
     * 		Reduce output records=3
     * 		Spilled Records=6
     * 		Shuffled Maps =3
     * 		Failed Shuffles=0
     * 		Merged Map outputs=3
     * 		GC time elapsed (ms)=12
     * 		Total committed heap usage (bytes)=1464336384
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=261
     * 	File Output Format Counters
     * 		Bytes Written=559
     *
     * Process finished with exit code 0
     */

}
