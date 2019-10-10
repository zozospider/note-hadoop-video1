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
 * 参考: [Hadoop CombineTextInputFormat 切片机制](https://blog.csdn.net/yljphp/article/details/89070948)
 */
public class CombineTextInputFormatDriver5 {

    /**
     * ➜  input ll /Users/user/other/tmp/mapReduce/input/combinetextinputformat5/
     * total 48496
     * -rw-r--r--  1 user  staff    79B 10 10 13:59 a
     * -rw-r--r--  1 user  staff   1.5M 10 10 13:59 b
     * -rw-r--r--  1 user  staff   5.1M 10 10 13:59 c
     * -rw-r--r--  1 user  staff   6.8M 10 10 13:59 d
     * -rw-r--r--  1 user  staff    10M 10 10 13:59 e
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat5/
     * ls: /Users/user/other/tmp/mapReduce/output/combinetextinputformat5/: No such file or directory
     * ➜  output
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/mapReduce/input/combinetextinputformat5", "/Users/user/other/tmp/mapReduce/output/combinetextinputformat5"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Map 输入类型为: CombineTextInputFormat, 且设置 maxInputSplitSize 为: 4 M
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CombineTextInputFormatDriver5.class);
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
     * ➜  output ll /Users/user/other/tmp/mapReduce/output/combinetextinputformat5/
     * total 8
     * -rw-r--r--  1 user  staff     0B 10 10 14:15 _SUCCESS
     * -rw-r--r--  1 user  staff   139B 10 10 14:15 part-r-00000
     * ➜  output cat combinetextinputformat5/part-r-00000
     * 	394
     * abc	628190
     * awesome	314014
     * book	628096
     * enough	314082
     * google	314016
     * love	942194
     * me	314082
     * please	314016
     * qq	628424
     * see	314014
     * who	314017
     * ➜  output
     */

    /**
     * 2019-10-10 14:15:04,060 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 14:15:04,400 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 14:15:04,401 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 14:15:04,961 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 14:15:04,966 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 14:15:05,006 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 5
     * 2019-10-10 14:15:05,026 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
     * 2019-10-10 14:15:05,064 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     * 2019-10-10 14:15:05,169 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local268875273_0001
     * 2019-10-10 14:15:05,367 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 14:15:05,368 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local268875273_0001
     * 2019-10-10 14:15:05,369 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 14:15:05,377 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:15:05,379 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 14:15:05,434 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 14:15:05,435 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local268875273_0001_m_000000_0
     * 2019-10-10 14:15:05,468 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:15:05,474 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:15:05,475 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:15:05,486 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/d:3580272+3580273,/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/e:0+4194304
     * 2019-10-10 14:15:05,554 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:15:05,554 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:15:05,554 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:15:05,554 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:15:05,554 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:15:05,556 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:15:06,371 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local268875273_0001 running in uber mode : false
     * 2019-10-10 14:15:06,372 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 14:15:06,486 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:15:06,486 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:15:06,488 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:15:06,488 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 14073960; bufvoid = 104857600
     * 2019-10-10 14:15:06,488 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 19915020(79660080); length = 6299377/6553600
     * 2019-10-10 14:15:07,425 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:15:07,447 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local268875273_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:15:07,456 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:15:07,456 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local268875273_0001_m_000000_0' done.
     * 2019-10-10 14:15:07,456 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local268875273_0001_m_000000_0
     * 2019-10-10 14:15:07,456 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local268875273_0001_m_000001_0
     * 2019-10-10 14:15:07,457 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:15:07,458 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:15:07,458 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:15:07,459 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/e:4194304+3272748,/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/e:7467052+3272748
     * 2019-10-10 14:15:07,494 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:15:07,494 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:15:07,494 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:15:07,494 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:15:07,494 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:15:07,494 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:15:08,146 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:15:08,146 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:15:08,146 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:15:08,146 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11848811; bufvoid = 104857600
     * 2019-10-10 14:15:08,146 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 20911076(83644304); length = 5303321/6553600
     * 2019-10-10 14:15:08,376 INFO [org.apache.hadoop.mapreduce.Job] -  map 25% reduce 0%
     * 2019-10-10 14:15:08,748 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:15:08,758 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local268875273_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 14:15:08,760 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:15:08,760 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local268875273_0001_m_000001_0' done.
     * 2019-10-10 14:15:08,760 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local268875273_0001_m_000001_0
     * 2019-10-10 14:15:08,761 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local268875273_0001_m_000002_0
     * 2019-10-10 14:15:08,762 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:15:08,762 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:15:08,762 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:15:08,763 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/c:2684950+2684950,/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/d:0+3580272
     * 2019-10-10 14:15:08,786 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:15:08,786 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:15:08,786 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:15:08,786 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:15:08,786 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:15:08,787 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:15:09,379 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 0%
     * 2019-10-10 14:15:09,481 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:15:09,481 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:15:09,481 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:15:09,481 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 11341179; bufvoid = 104857600
     * 2019-10-10 14:15:09,481 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 21138444(84553776); length = 5075953/6553600
     * 2019-10-10 14:15:10,026 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:15:10,039 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local268875273_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 14:15:10,041 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:15:10,041 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local268875273_0001_m_000002_0' done.
     * 2019-10-10 14:15:10,041 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local268875273_0001_m_000002_0
     * 2019-10-10 14:15:10,041 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local268875273_0001_m_000003_0
     * 2019-10-10 14:15:10,043 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:15:10,043 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:15:10,043 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:15:10,045 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: Paths:/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/a:0+79,/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/b:0+1540832,/Users/user/other/tmp/mapReduce/input/combinetextinputformat5/c:0+2684950
     * 2019-10-10 14:15:10,066 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 14:15:10,067 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 14:15:10,067 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 14:15:10,067 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 14:15:10,067 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 14:15:10,067 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 14:15:10,534 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 14:15:10,534 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 14:15:10,534 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 14:15:10,534 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 7649362; bufvoid = 104857600
     * 2019-10-10 14:15:10,534 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 22790904(91163616); length = 3423493/6553600
     * 2019-10-10 14:15:10,908 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 14:15:10,922 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local268875273_0001_m_000003_0 is done. And is in the process of committing
     * 2019-10-10 14:15:10,923 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 14:15:10,923 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local268875273_0001_m_000003_0' done.
     * 2019-10-10 14:15:10,923 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local268875273_0001_m_000003_0
     * 2019-10-10 14:15:10,923 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 14:15:10,928 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 14:15:10,928 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local268875273_0001_r_000000_0
     * 2019-10-10 14:15:10,935 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 14:15:10,935 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 14:15:10,935 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 14:15:10,937 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@4c0ec24f
     * 2019-10-10 14:15:10,948 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 14:15:10,950 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local268875273_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 14:15:10,993 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local268875273_0001_m_000002_0 decomp: 13879159 len: 13879163 to MEMORY
     * 2019-10-10 14:15:11,016 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 13879159 bytes from map-output for attempt_local268875273_0001_m_000002_0
     * 2019-10-10 14:15:11,018 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 13879159, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->13879159
     * 2019-10-10 14:15:11,024 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local268875273_0001_m_000000_0 decomp: 17223652 len: 17223656 to MEMORY
     * 2019-10-10 14:15:11,049 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 17223652 bytes from map-output for attempt_local268875273_0001_m_000000_0
     * 2019-10-10 14:15:11,049 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 17223652, inMemoryMapOutputs.size() -> 2, commitMemory -> 13879159, usedMemory ->31102811
     * 2019-10-10 14:15:11,052 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local268875273_0001_m_000003_0 decomp: 9361112 len: 9361116 to MEMORY
     * 2019-10-10 14:15:11,064 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 9361112 bytes from map-output for attempt_local268875273_0001_m_000003_0
     * 2019-10-10 14:15:11,065 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 9361112, inMemoryMapOutputs.size() -> 3, commitMemory -> 31102811, usedMemory ->40463923
     * 2019-10-10 14:15:11,069 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local268875273_0001_m_000001_0 decomp: 14500475 len: 14500479 to MEMORY
     * 2019-10-10 14:15:11,086 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 14500475 bytes from map-output for attempt_local268875273_0001_m_000001_0
     * 2019-10-10 14:15:11,086 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 14500475, inMemoryMapOutputs.size() -> 4, commitMemory -> 40463923, usedMemory ->54964398
     * 2019-10-10 14:15:11,087 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 14:15:11,087 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 14:15:11,087 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 4 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 14:15:11,094 INFO [org.apache.hadoop.mapred.Merger] - Merging 4 sorted segments
     * 2019-10-10 14:15:11,094 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 4 segments left of total size: 54964383 bytes
     * 2019-10-10 14:15:12,653 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 4 segments, 54964398 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 14:15:12,653 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 54964396 bytes from disk
     * 2019-10-10 14:15:12,654 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 14:15:12,654 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 14:15:12,654 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 54964389 bytes
     * 2019-10-10 14:15:12,655 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 14:15:12,667 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 14:15:14,319 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local268875273_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 14:15:14,320 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 4 / 4 copied.
     * 2019-10-10 14:15:14,320 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local268875273_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 14:15:14,321 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local268875273_0001_r_000000_0' to file:/Users/user/other/tmp/mapReduce/output/combinetextinputformat5/_temporary/0/task_local268875273_0001_r_000000
     * 2019-10-10 14:15:14,321 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 14:15:14,322 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local268875273_0001_r_000000_0' done.
     * 2019-10-10 14:15:14,322 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local268875273_0001_r_000000_0
     * 2019-10-10 14:15:14,322 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 14:15:14,391 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 14:15:14,392 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local268875273_0001 completed successfully
     * 2019-10-10 14:15:14,407 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=202334702
     * 		FILE: Number of bytes written=260833997
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=1885025
     * 		Map output records=5025539
     * 		Map output bytes=44913312
     * 		Map output materialized bytes=54964414
     * 		Input split bytes=1065
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=12
     * 		Reduce shuffle bytes=54964414
     * 		Reduce input records=5025539
     * 		Reduce output records=12
     * 		Spilled Records=10051078
     * 		Shuffled Maps =4
     * 		Failed Shuffles=0
     * 		Merged Map outputs=4
     * 		GC time elapsed (ms)=425
     * 		Total committed heap usage (bytes)=3862429696
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
     * 		Bytes Written=151
     *
     * Process finished with exit code 0
     */

}
