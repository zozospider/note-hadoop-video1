package com.zozospider.hadoop.mapreduce.wordcount;

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
public class WordCountDriver {

    /**
     * spiderxmac:wordCount zoz$ ls -l /Users/zoz/zz/other/tmp/input/wordCount/
     * total 8
     * -rw-r--r--@ 1 zoz  staff  82 10  6 12:24 inputWords
     * spiderxmac:wordCount zoz$ cat inputWords
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * endspiderxmac:wordCount zoz$
     *
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/output/
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        // args = new String[]{"/Users/zoz/zz/other/tmp/input/wordCount", "/Users/zoz/zz/other/tmp/output/wordCount"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

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
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/output/
     * total 0
     * drwxr-xr-x  6 zoz  staff  192 10  6 14:12 wordCount
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/output/wordCount/
     * total 8
     * -rw-r--r--  1 zoz  staff   0 10  6 14:12 _SUCCESS
     * -rw-r--r--  1 zoz  staff  85 10  6 14:12 part-r-00000
     * spiderxmac:output zoz$ cat wordCount/part-r-00000
     * abc	2
     * awesome	1
     * book	2
     * end	1
     * enough	1
     * google	1
     * love	3
     * me	1
     * please	1
     * qq	2
     * see	1
     * who	1
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-06 14:12:06,734 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-06 14:12:06,884 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-06 14:12:06,884 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-06 14:12:07,156 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-06 14:12:07,162 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-06 14:12:07,179 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-06 14:12:07,226 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-06 14:12:07,312 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local901406321_0001
     * 2019-10-06 14:12:07,446 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-06 14:12:07,447 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local901406321_0001
     * 2019-10-06 14:12:07,447 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-06 14:12:07,450 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-06 14:12:07,451 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-06 14:12:07,474 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-06 14:12:07,475 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local901406321_0001_m_000000_0
     * 2019-10-06 14:12:07,491 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-06 14:12:07,495 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-06 14:12:07,495 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-06 14:12:07,498 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/input/wordCount/inputWords:0+82
     * 2019-10-06 14:12:07,549 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-06 14:12:07,549 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-06 14:12:07,549 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-06 14:12:07,549 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-06 14:12:07,549 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-06 14:12:07,551 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-06 14:12:07,556 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-06 14:12:07,556 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-06 14:12:07,556 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-06 14:12:07,556 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 151; bufvoid = 104857600
     * 2019-10-06 14:12:07,556 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214332(104857328); length = 65/6553600
     * 2019-10-06 14:12:07,561 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-06 14:12:07,563 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local901406321_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-06 14:12:07,569 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-06 14:12:07,569 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local901406321_0001_m_000000_0' done.
     * 2019-10-06 14:12:07,569 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local901406321_0001_m_000000_0
     * 2019-10-06 14:12:07,569 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-06 14:12:07,571 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-06 14:12:07,571 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local901406321_0001_r_000000_0
     * 2019-10-06 14:12:07,575 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-06 14:12:07,575 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-06 14:12:07,575 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-06 14:12:07,577 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@dccfe8c
     * 2019-10-06 14:12:07,585 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-06 14:12:07,587 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local901406321_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-06 14:12:07,613 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local901406321_0001_m_000000_0 decomp: 187 len: 191 to MEMORY
     * 2019-10-06 14:12:07,624 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 187 bytes from map-output for attempt_local901406321_0001_m_000000_0
     * 2019-10-06 14:12:07,625 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 187, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->187
     * 2019-10-06 14:12:07,625 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-06 14:12:07,626 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-06 14:12:07,626 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-06 14:12:07,630 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-06 14:12:07,630 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 181 bytes
     * 2019-10-06 14:12:07,631 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 187 bytes to disk to satisfy reduce memory limit
     * 2019-10-06 14:12:07,631 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 191 bytes from disk
     * 2019-10-06 14:12:07,631 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-06 14:12:07,631 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-06 14:12:07,632 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 181 bytes
     * 2019-10-06 14:12:07,632 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-06 14:12:07,643 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-06 14:12:07,647 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local901406321_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-06 14:12:07,648 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-06 14:12:07,648 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local901406321_0001_r_000000_0 is allowed to commit now
     * 2019-10-06 14:12:07,648 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local901406321_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/output/wordCount/_temporary/0/task_local901406321_0001_r_000000
     * 2019-10-06 14:12:07,649 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-06 14:12:07,649 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local901406321_0001_r_000000_0' done.
     * 2019-10-06 14:12:07,649 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local901406321_0001_r_000000_0
     * 2019-10-06 14:12:07,649 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-06 14:12:08,453 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local901406321_0001 running in uber mode : false
     * 2019-10-06 14:12:08,454 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-06 14:12:08,455 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local901406321_0001 completed successfully
     * 2019-10-06 14:12:08,461 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=926
     * 		FILE: Number of bytes written=550010
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=7
     * 		Map output records=17
     * 		Map output bytes=151
     * 		Map output materialized bytes=191
     * 		Input split bytes=120
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=12
     * 		Reduce shuffle bytes=191
     * 		Reduce input records=17
     * 		Reduce output records=12
     * 		Spilled Records=34
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=7
     * 		Total committed heap usage (bytes)=514850816
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=82
     * 	File Output Format Counters
     * 		Bytes Written=97
     *
     * Process finished with exit code 0
     */

}
