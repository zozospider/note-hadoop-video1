package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动:
 */
public class TopN2Driver {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/TopN2/
     * total 8
     * -rw-r--r--  1 zoz  staff  99 10 27 20:50 f1
     * spiderxmac:input zoz$ cat TopN2/f1
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
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/TopN2/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/TopN2/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/TopN2", "/Users/zoz/zz/other/tmp/MapReduce/output/TopN2"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(TopN2Driver.class);
        job.setMapperClass(TopN2Mapper.class);
        job.setReducerClass(TopN2Reducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(TopN2KeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TopN2KeyWritable.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/TopN2/
     * total 8
     * -rw-r--r--  1 zoz  staff    0 10 27 20:52 _SUCCESS
     * -rw-r--r--  1 zoz  staff  503 10 27 20:52 part-r-00000
     * spiderxmac:output zoz$ cat TopN2/part-r-00000
     * TopN2KeyWritable{field1=1, field2=99, fieldSum=198}	ss
     * TopN2KeyWritable{field1=1, field2=80, fieldSum=160}	what
     * TopN2KeyWritable{field1=40, field2=25, fieldSum=50}	zoo
     * TopN2KeyWritable{field1=50, field2=20, fieldSum=40}	xpp
     * TopN2KeyWritable{field1=10, field2=15, fieldSum=30}	abc
     * TopN2KeyWritable{field1=30, field2=10, fieldSum=20}	love
     * TopN2KeyWritable{field1=100, field2=3, fieldSum=6}	book
     * TopN2KeyWritable{field1=99, field2=1, fieldSum=2}	good
     * TopN2KeyWritable{field1=100, field2=0, fieldSum=0}	zoo
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-27 20:52:19,120 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-27 20:52:19,207 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-27 20:52:19,208 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-27 20:52:19,618 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-27 20:52:19,623 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-27 20:52:19,636 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-27 20:52:19,676 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-27 20:52:19,742 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1120393240_0001
     * 2019-10-27 20:52:19,839 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-27 20:52:19,840 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1120393240_0001
     * 2019-10-27 20:52:19,840 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-27 20:52:19,843 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-27 20:52:19,844 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-27 20:52:19,868 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-27 20:52:19,868 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1120393240_0001_m_000000_0
     * 2019-10-27 20:52:19,883 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-27 20:52:19,887 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-27 20:52:19,887 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-27 20:52:19,890 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/TopN2/f1:0+99
     * 2019-10-27 20:52:19,940 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-27 20:52:19,940 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-27 20:52:19,940 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-27 20:52:19,940 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-27 20:52:19,940 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-27 20:52:19,942 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-27 20:52:19,947 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-27 20:52:19,947 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-27 20:52:19,947 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-27 20:52:19,947 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 147; bufvoid = 104857600
     * 2019-10-27 20:52:19,947 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214364(104857456); length = 33/6553600
     * 2019-10-27 20:52:19,951 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-27 20:52:19,953 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1120393240_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-27 20:52:19,958 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-27 20:52:19,958 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1120393240_0001_m_000000_0' done.
     * 2019-10-27 20:52:19,958 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1120393240_0001_m_000000_0
     * 2019-10-27 20:52:19,958 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-27 20:52:19,960 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-27 20:52:19,960 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1120393240_0001_r_000000_0
     * 2019-10-27 20:52:19,964 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-27 20:52:19,964 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-27 20:52:19,964 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-27 20:52:19,966 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@4b5fdbfb
     * 2019-10-27 20:52:19,975 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-27 20:52:19,977 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1120393240_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-27 20:52:20,003 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1120393240_0001_m_000000_0 decomp: 167 len: 171 to MEMORY
     * 2019-10-27 20:52:20,013 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 167 bytes from map-output for attempt_local1120393240_0001_m_000000_0
     * 2019-10-27 20:52:20,013 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 167, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->167
     * 2019-10-27 20:52:20,014 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-27 20:52:20,014 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-27 20:52:20,015 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-27 20:52:20,018 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-27 20:52:20,019 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 153 bytes
     * 2019-10-27 20:52:20,019 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 167 bytes to disk to satisfy reduce memory limit
     * 2019-10-27 20:52:20,019 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 171 bytes from disk
     * 2019-10-27 20:52:20,020 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-27 20:52:20,020 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-27 20:52:20,021 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 153 bytes
     * 2019-10-27 20:52:20,021 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-27 20:52:20,031 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-27 20:52:20,035 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1120393240_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-27 20:52:20,036 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-27 20:52:20,036 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1120393240_0001_r_000000_0 is allowed to commit now
     * 2019-10-27 20:52:20,036 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1120393240_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/TopN2/_temporary/0/task_local1120393240_0001_r_000000
     * 2019-10-27 20:52:20,037 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-27 20:52:20,037 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1120393240_0001_r_000000_0' done.
     * 2019-10-27 20:52:20,037 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1120393240_0001_r_000000_0
     * 2019-10-27 20:52:20,037 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-27 20:52:20,844 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1120393240_0001 running in uber mode : false
     * 2019-10-27 20:52:20,845 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-27 20:52:20,846 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1120393240_0001 completed successfully
     * 2019-10-27 20:52:20,853 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=916
     * 		FILE: Number of bytes written=553488
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=10
     * 		Map output records=9
     * 		Map output bytes=147
     * 		Map output materialized bytes=171
     * 		Input split bytes=118
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=9
     * 		Reduce shuffle bytes=171
     * 		Reduce input records=9
     * 		Reduce output records=9
     * 		Spilled Records=18
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
     * 		Bytes Read=99
     * 	File Output Format Counters
     * 		Bytes Written=515
     *
     * Process finished with exit code 0
     */

}
