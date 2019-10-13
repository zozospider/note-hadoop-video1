package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 找出 field1 相同的多行中 field2 最大的前 2 行
 */
public class GroupingComparatorDriver2 {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/GroupingComparator2/
     * total 8
     * -rw-r--r--  1 zoz  staff  159 10 13 17:45 f1
     * spiderxmac:input zoz$ cat GroupingComparator2/f1
     * one 1 100
     * two 2 500
     * five 5 500
     * one. 1 300
     * three 3 200
     * one.. 1 200
     * seven 7 800
     * four 4 800
     * six 6 700
     * five. 5 500
     * nine 9 200
     * nine. 9 300
     * eight 8 100
     * nine.. 9 900
     * spiderxmac:input zoz$
     * <p>
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator2/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator2/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/GroupingComparator2", "/Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator2"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 GroupingComparator 为 GroupingComparatorKeyComparator
        job.setGroupingComparatorClass(GroupingComparatorKeyComparator.class);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(GroupingComparatorDriver2.class);
        job.setMapperClass(GroupingComparatorMapper.class);
        job.setReducerClass(GroupingComparatorReducer2.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(GroupingComparatorKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(GroupingComparatorKeyWritable.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator2/
     * total 8
     * -rw-r--r--  1 zoz  staff    0 10 13 17:56 _SUCCESS
     * -rw-r--r--  1 zoz  staff  690 10 13 17:56 part-r-00000
     * spiderxmac:output zoz$ cat GroupingComparator2/part-r-00000
     * GroupingComparatorKeyWritable{field1=1, field2=300}	one.
     * GroupingComparatorKeyWritable{field1=1, field2=200}	one..
     * GroupingComparatorKeyWritable{field1=2, field2=500}	two
     * GroupingComparatorKeyWritable{field1=3, field2=200}	three
     * GroupingComparatorKeyWritable{field1=4, field2=800}	four
     * GroupingComparatorKeyWritable{field1=5, field2=500}	five
     * GroupingComparatorKeyWritable{field1=5, field2=500}	five.
     * GroupingComparatorKeyWritable{field1=6, field2=700}	six
     * GroupingComparatorKeyWritable{field1=7, field2=800}	seven
     * GroupingComparatorKeyWritable{field1=8, field2=100}	eight
     * GroupingComparatorKeyWritable{field1=9, field2=900}	nine..
     * GroupingComparatorKeyWritable{field1=9, field2=300}	nine.
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-13 17:56:50,558 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-13 17:56:50,711 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-13 17:56:50,721 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-13 17:56:50,964 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-13 17:56:50,970 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-13 17:56:50,983 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-13 17:56:51,024 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-13 17:56:51,097 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local23666108_0001
     * 2019-10-13 17:56:51,219 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-13 17:56:51,222 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-13 17:56:51,223 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-13 17:56:51,229 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-13 17:56:51,229 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local23666108_0001
     * 2019-10-13 17:56:51,247 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-13 17:56:51,248 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local23666108_0001_m_000000_0
     * 2019-10-13 17:56:51,264 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-13 17:56:51,268 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-13 17:56:51,268 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-13 17:56:51,271 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/GroupingComparator2/f1:0+159
     * 2019-10-13 17:56:51,321 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-13 17:56:51,321 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-13 17:56:51,321 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-13 17:56:51,321 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-13 17:56:51,321 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-13 17:56:51,323 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-13 17:56:51,327 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-13 17:56:51,327 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-13 17:56:51,327 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-13 17:56:51,327 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 187; bufvoid = 104857600
     * 2019-10-13 17:56:51,327 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214344(104857376); length = 53/6553600
     * 2019-10-13 17:56:51,331 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-13 17:56:51,334 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local23666108_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-13 17:56:51,338 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-13 17:56:51,338 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local23666108_0001_m_000000_0' done.
     * 2019-10-13 17:56:51,338 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local23666108_0001_m_000000_0
     * 2019-10-13 17:56:51,338 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-13 17:56:51,340 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-13 17:56:51,340 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local23666108_0001_r_000000_0
     * 2019-10-13 17:56:51,344 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-13 17:56:51,344 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-13 17:56:51,344 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-13 17:56:51,346 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@71ae53b7
     * 2019-10-13 17:56:51,353 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-13 17:56:51,355 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local23666108_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-13 17:56:51,382 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local23666108_0001_m_000000_0 decomp: 217 len: 221 to MEMORY
     * 2019-10-13 17:56:51,393 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 217 bytes from map-output for attempt_local23666108_0001_m_000000_0
     * 2019-10-13 17:56:51,394 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 217, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->217
     * 2019-10-13 17:56:51,394 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-13 17:56:51,395 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-13 17:56:51,395 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-13 17:56:51,398 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-13 17:56:51,399 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 207 bytes
     * 2019-10-13 17:56:51,399 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 217 bytes to disk to satisfy reduce memory limit
     * 2019-10-13 17:56:51,400 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 221 bytes from disk
     * 2019-10-13 17:56:51,400 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-13 17:56:51,400 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-13 17:56:51,401 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 207 bytes
     * 2019-10-13 17:56:51,401 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-13 17:56:51,411 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-13 17:56:51,415 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local23666108_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-13 17:56:51,416 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-13 17:56:51,416 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local23666108_0001_r_000000_0 is allowed to commit now
     * 2019-10-13 17:56:51,417 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local23666108_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator2/_temporary/0/task_local23666108_0001_r_000000
     * 2019-10-13 17:56:51,417 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-13 17:56:51,417 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local23666108_0001_r_000000_0' done.
     * 2019-10-13 17:56:51,417 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local23666108_0001_r_000000_0
     * 2019-10-13 17:56:51,417 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-13 17:56:52,235 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local23666108_0001 running in uber mode : false
     * 2019-10-13 17:56:52,236 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-13 17:56:52,237 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local23666108_0001 completed successfully
     * 2019-10-13 17:56:52,243 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=1166
     * 		FILE: Number of bytes written=549475
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=14
     * 		Map output records=14
     * 		Map output bytes=187
     * 		Map output materialized bytes=221
     * 		Input split bytes=132
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=9
     * 		Reduce shuffle bytes=221
     * 		Reduce input records=14
     * 		Reduce output records=12
     * 		Spilled Records=28
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
     * 		Bytes Read=159
     * 	File Output Format Counters
     * 		Bytes Written=706
     *
     * Process finished with exit code 0
     */

}
