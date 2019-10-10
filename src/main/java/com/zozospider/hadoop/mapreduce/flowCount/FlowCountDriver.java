package com.zozospider.hadoop.mapreduce.flowCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动
 */
public class FlowCountDriver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/FlowCount/
     * total 8
     * -rw-r--r--  1 user  staff   399B 10 10 16:26 f1
     * ➜  input cat FlowCount/f1
     * 1|13366999900|192.168.1.0|Mi8SE|www.meituan.com|5636|7788|200
     * 2|13722065599|192.168.2.1|Mi Phone|www.microsoft.com|2481|2306|200
     * 3|13366999900|192.168.1.0|Mi8SE|www.meituan.com|1802|2380|200
     * 4|15933022206|192.168.100.6|Huawei||264|0|200
     * 5|16955332200|192.168.2.15|Sumsung Galaxy|www.qq.com|997|2233|200
     * 6|18322007788|192.168.3.16|Huawei||0|264|200
     * 7|18322007788|192.168.3.16|iPhone 10||3955|326|200
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/FlowCount/
     * ls: /Users/user/other/tmp/MapReduce/output/FlowCount/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/FlowCount", "/Users/user/other/tmp/MapReduce/output/FlowCount"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(FlowCountDriver.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowCountValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowCountValueWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/FlowCount/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 16:29 _SUCCESS
     * -rw-r--r--  1 user  staff   380B 10 10 16:29 part-r-00000
     * ➜  output cat FlowCount/part-r-00000
     * 13366999900	FlowCountValueWritable{upFlow=7438, downFlow=10168, sumFlow=17606}
     * 13722065599	FlowCountValueWritable{upFlow=2481, downFlow=2306, sumFlow=4787}
     * 15933022206	FlowCountValueWritable{upFlow=264, downFlow=0, sumFlow=264}
     * 16955332200	FlowCountValueWritable{upFlow=997, downFlow=2233, sumFlow=3230}
     * 18322007788	FlowCountValueWritable{upFlow=3955, downFlow=590, sumFlow=4545}
     * ➜  output
     */

    /**
     * 2019-10-10 16:29:26,286 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 16:29:26,617 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 16:29:26,618 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 16:29:27,260 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 16:29:27,265 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 16:29:27,282 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-10 16:29:27,329 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-10 16:29:27,445 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1753008176_0001
     * 2019-10-10 16:29:27,641 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 16:29:27,642 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1753008176_0001
     * 2019-10-10 16:29:27,643 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 16:29:27,650 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:29:27,652 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 16:29:27,702 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 16:29:27,703 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1753008176_0001_m_000000_0
     * 2019-10-10 16:29:27,731 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:29:27,740 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:29:27,740 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:29:27,746 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/FlowCount/f1:0+399
     * 2019-10-10 16:29:27,805 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 16:29:27,805 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 16:29:27,805 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 16:29:27,805 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 16:29:27,805 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 16:29:27,808 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 16:29:27,817 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 16:29:27,817 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 16:29:27,817 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 16:29:27,817 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 252; bufvoid = 104857600
     * 2019-10-10 16:29:27,817 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214372(104857488); length = 25/6553600
     * 2019-10-10 16:29:27,825 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 16:29:27,829 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1753008176_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:29:27,835 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 16:29:27,836 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1753008176_0001_m_000000_0' done.
     * 2019-10-10 16:29:27,836 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1753008176_0001_m_000000_0
     * 2019-10-10 16:29:27,836 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 16:29:27,838 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 16:29:27,838 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1753008176_0001_r_000000_0
     * 2019-10-10 16:29:27,845 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 16:29:27,846 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 16:29:27,846 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 16:29:27,850 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@7fbc2c3
     * 2019-10-10 16:29:27,860 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 16:29:27,863 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1753008176_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 16:29:27,901 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1753008176_0001_m_000000_0 decomp: 268 len: 272 to MEMORY
     * 2019-10-10 16:29:27,914 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 268 bytes from map-output for attempt_local1753008176_0001_m_000000_0
     * 2019-10-10 16:29:27,916 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 268, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->268
     * 2019-10-10 16:29:27,917 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 16:29:27,918 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 16:29:27,919 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 16:29:27,926 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:29:27,926 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 254 bytes
     * 2019-10-10 16:29:27,927 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 268 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 16:29:27,928 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 272 bytes from disk
     * 2019-10-10 16:29:27,929 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 16:29:27,929 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 16:29:27,929 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 254 bytes
     * 2019-10-10 16:29:27,930 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 16:29:27,943 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 16:29:27,962 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1753008176_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 16:29:27,964 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-10 16:29:27,964 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1753008176_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 16:29:27,965 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1753008176_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/FlowCount/_temporary/0/task_local1753008176_0001_r_000000
     * 2019-10-10 16:29:27,966 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 16:29:27,966 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1753008176_0001_r_000000_0' done.
     * 2019-10-10 16:29:27,967 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1753008176_0001_r_000000_0
     * 2019-10-10 16:29:27,967 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 16:29:28,650 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1753008176_0001 running in uber mode : false
     * 2019-10-10 16:29:28,651 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 16:29:28,651 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1753008176_0001 completed successfully
     * 2019-10-10 16:29:28,664 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=1726
     * 		FILE: Number of bytes written=556836
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=7
     * 		Map output records=7
     * 		Map output bytes=252
     * 		Map output materialized bytes=272
     * 		Input split bytes=120
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=5
     * 		Reduce shuffle bytes=272
     * 		Reduce input records=7
     * 		Reduce output records=5
     * 		Spilled Records=14
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=10
     * 		Total committed heap usage (bytes)=468713472
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=399
     * 	File Output Format Counters
     * 		Bytes Written=392
     *
     * Process finished with exit code 0
     */
}
