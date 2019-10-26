package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 找出每个单词的位置 (包括所在文件和当前文件的偏移量)
 */
public class MultiJob1Driver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/MultiJob/one/
     * total 24
     * -rw-r--r--  1 user  staff    24B 10 26 16:42 f1
     * -rw-r--r--  1 user  staff    24B 10 25 21:32 f2
     * -rw-r--r--  1 user  staff    20B 10 26 16:45 f3
     * ➜  input cat MultiJob/one/f1
     * abc zoo abc
     * zoo why
     * abc
     * ➜  input cat MultiJob/one/f2
     * why zoo zoo why
     * abc why
     * ➜  input cat MultiJob/one/f3
     * zoo why
     * abc abc
     * why
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/MultiJob/one/
     * ls: /Users/user/other/tmp/MapReduce/output/MultiJob/one/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/MultiJob/one", "/Users/user/other/tmp/MapReduce/output/MultiJob/one"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(MultiJob1Driver.class);
        job.setMapperClass(MultiJob1Mapper.class);
        job.setReducerClass(MultiJob1Reducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/MultiJob/one/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 26 17:33 _SUCCESS
     * -rw-r--r--  1 user  staff   105B 10 26 17:33 part-r-00000
     * ➜  output cat MultiJob/one/part-r-00000
     * abc	f3,12|f3,8|f1,20|f1,8|f1,0|f2,16
     * why	f2,20|f2,12|f2,0|f3,16|f3,4|f1,16
     * zoo	f1,12|f1,4|f3,0|f2,8|f2,4
     * ➜  output
     */

    /**
     * 2019-10-26 17:33:16,312 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-26 17:33:16,638 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-26 17:33:16,640 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-26 17:33:17,289 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-26 17:33:17,293 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-26 17:33:17,321 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-26 17:33:17,370 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-26 17:33:17,482 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local403947069_0001
     * 2019-10-26 17:33:17,703 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-26 17:33:17,705 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local403947069_0001
     * 2019-10-26 17:33:17,705 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-26 17:33:17,709 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 17:33:17,711 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-26 17:33:17,755 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-26 17:33:17,760 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local403947069_0001_m_000000_0
     * 2019-10-26 17:33:17,793 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 17:33:17,801 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-26 17:33:17,801 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-26 17:33:17,806 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/MultiJob/one/f1:0+24
     * 2019-10-26 17:33:17,876 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-26 17:33:17,876 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-26 17:33:17,876 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-26 17:33:17,876 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-26 17:33:17,876 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-26 17:33:17,879 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-26 17:33:17,886 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-26 17:33:17,887 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-26 17:33:17,887 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-26 17:33:17,887 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 57; bufvoid = 104857600
     * 2019-10-26 17:33:17,887 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214376(104857504); length = 21/6553600
     * 2019-10-26 17:33:17,893 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-26 17:33:17,896 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local403947069_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-26 17:33:17,905 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-26 17:33:17,905 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local403947069_0001_m_000000_0' done.
     * 2019-10-26 17:33:17,905 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local403947069_0001_m_000000_0
     * 2019-10-26 17:33:17,905 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local403947069_0001_m_000001_0
     * 2019-10-26 17:33:17,906 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 17:33:17,907 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-26 17:33:17,907 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-26 17:33:17,909 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/MultiJob/one/f2:0+24
     * 2019-10-26 17:33:17,956 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-26 17:33:17,957 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-26 17:33:17,957 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-26 17:33:17,957 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-26 17:33:17,957 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-26 17:33:17,957 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-26 17:33:17,959 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-26 17:33:17,959 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-26 17:33:17,959 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-26 17:33:17,959 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 57; bufvoid = 104857600
     * 2019-10-26 17:33:17,959 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214376(104857504); length = 21/6553600
     * 2019-10-26 17:33:17,961 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-26 17:33:17,963 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local403947069_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-26 17:33:17,965 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-26 17:33:17,965 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local403947069_0001_m_000001_0' done.
     * 2019-10-26 17:33:17,965 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local403947069_0001_m_000001_0
     * 2019-10-26 17:33:17,965 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local403947069_0001_m_000002_0
     * 2019-10-26 17:33:17,966 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 17:33:17,967 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-26 17:33:17,967 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-26 17:33:17,968 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/MultiJob/one/f3:0+20
     * 2019-10-26 17:33:18,021 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-26 17:33:18,021 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-26 17:33:18,021 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-26 17:33:18,021 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-26 17:33:18,021 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-26 17:33:18,022 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-26 17:33:18,023 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-26 17:33:18,023 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-26 17:33:18,024 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-26 17:33:18,024 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 47; bufvoid = 104857600
     * 2019-10-26 17:33:18,024 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214380(104857520); length = 17/6553600
     * 2019-10-26 17:33:18,025 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-26 17:33:18,026 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local403947069_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-26 17:33:18,028 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-26 17:33:18,028 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local403947069_0001_m_000002_0' done.
     * 2019-10-26 17:33:18,028 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local403947069_0001_m_000002_0
     * 2019-10-26 17:33:18,028 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-26 17:33:18,030 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-26 17:33:18,030 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local403947069_0001_r_000000_0
     * 2019-10-26 17:33:18,037 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-26 17:33:18,037 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-26 17:33:18,037 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-26 17:33:18,039 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@18c53612
     * 2019-10-26 17:33:18,049 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-26 17:33:18,052 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local403947069_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-26 17:33:18,090 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local403947069_0001_m_000000_0 decomp: 71 len: 75 to MEMORY
     * 2019-10-26 17:33:18,104 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 71 bytes from map-output for attempt_local403947069_0001_m_000000_0
     * 2019-10-26 17:33:18,105 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 71, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->71
     * 2019-10-26 17:33:18,107 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local403947069_0001_m_000002_0 decomp: 59 len: 63 to MEMORY
     * 2019-10-26 17:33:18,107 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 59 bytes from map-output for attempt_local403947069_0001_m_000002_0
     * 2019-10-26 17:33:18,108 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 59, inMemoryMapOutputs.size() -> 2, commitMemory -> 71, usedMemory ->130
     * 2019-10-26 17:33:18,109 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local403947069_0001_m_000001_0 decomp: 71 len: 75 to MEMORY
     * 2019-10-26 17:33:18,110 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 71 bytes from map-output for attempt_local403947069_0001_m_000001_0
     * 2019-10-26 17:33:18,110 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 71, inMemoryMapOutputs.size() -> 3, commitMemory -> 130, usedMemory ->201
     * 2019-10-26 17:33:18,110 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-26 17:33:18,111 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-26 17:33:18,126 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-26 17:33:18,134 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-26 17:33:18,135 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 183 bytes
     * 2019-10-26 17:33:18,136 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 201 bytes to disk to satisfy reduce memory limit
     * 2019-10-26 17:33:18,136 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 201 bytes from disk
     * 2019-10-26 17:33:18,137 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-26 17:33:18,137 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-26 17:33:18,137 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 191 bytes
     * 2019-10-26 17:33:18,138 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-26 17:33:18,152 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-26 17:33:18,160 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local403947069_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-26 17:33:18,162 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-26 17:33:18,162 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local403947069_0001_r_000000_0 is allowed to commit now
     * 2019-10-26 17:33:18,163 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local403947069_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/MultiJob/one/_temporary/0/task_local403947069_0001_r_000000
     * 2019-10-26 17:33:18,164 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-26 17:33:18,164 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local403947069_0001_r_000000_0' done.
     * 2019-10-26 17:33:18,164 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local403947069_0001_r_000000_0
     * 2019-10-26 17:33:18,164 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-26 17:33:18,712 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local403947069_0001 running in uber mode : false
     * 2019-10-26 17:33:18,713 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-26 17:33:18,713 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local403947069_0001 completed successfully
     * 2019-10-26 17:33:18,727 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=4462
     * 		FILE: Number of bytes written=1106901
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=8
     * 		Map output records=17
     * 		Map output bytes=161
     * 		Map output materialized bytes=213
     * 		Input split bytes=369
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=3
     * 		Reduce shuffle bytes=213
     * 		Reduce input records=17
     * 		Reduce output records=3
     * 		Spilled Records=34
     * 		Shuffled Maps =3
     * 		Failed Shuffles=0
     * 		Merged Map outputs=3
     * 		GC time elapsed (ms)=14
     * 		Total committed heap usage (bytes)=1464336384
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=68
     * 	File Output Format Counters
     * 		Bytes Written=117
     *
     * Process finished with exit code 0
     */

}
