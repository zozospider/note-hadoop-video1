package com.zozospider.hadoop.mapreduce.compress.local;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 本地压缩与解压缩数据
 */
public class LocalCompress {

    /**
     * ➜  compress ll /Users/user/other/tmp/MapReduce/compress/
     * total 8
     * -rw-r--r--  1 user  staff    79B 10 17 20:53 f1
     * ➜  compress cat f1
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * ➜  compress
     * <p>
     * <p>
     * ➜  compress ll /Users/user/other/tmp/MapReduce/compress/
     * total 16
     * -rw-r--r--  1 user  staff    79B 10 17 20:53 f1
     * -rw-r--r--  1 user  staff    80B 10 17 21:10 f1.gz
     * ➜  compress
     * <p>
     * <p>
     * ➜  compress ll /Users/user/other/tmp/MapReduce/compress/
     * total 24
     * -rw-r--r--  1 user  staff    79B 10 17 20:53 f1
     * -rw-r--r--  1 user  staff    91B 10 17 21:11 f1.bz2
     * -rw-r--r--  1 user  staff    80B 10 17 21:10 f1.gz
     * ➜  compress
     * <p>
     * <p>
     * ➜  compress ll /Users/user/other/tmp/MapReduce/compress/
     * total 32
     * -rw-r--r--  1 user  staff    79B 10 17 20:53 f1
     * -rw-r--r--  1 user  staff    91B 10 17 21:11 f1.bz2
     * -rw-r--r--  1 user  staff    80B 10 17 21:10 f1.gz
     * -rw-r--r--  1 user  staff    79B 10 17 21:11 f1.gz.decode
     * ➜  compress cat f1.gz.decode
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * ➜  compress
     * <p>
     * <p>
     * ➜  compress ll /Users/user/other/tmp/MapReduce/compress/
     * total 40
     * -rw-r--r--  1 user  staff    79B 10 17 20:53 f1
     * -rw-r--r--  1 user  staff    91B 10 17 21:11 f1.bz2
     * -rw-r--r--  1 user  staff    79B 10 17 21:12 f1.bz2.decode
     * -rw-r--r--  1 user  staff    80B 10 17 21:10 f1.gz
     * -rw-r--r--  1 user  staff    79B 10 17 21:11 f1.gz.decode
     * ➜  compress cat f1.bz2.decode
     * abc abc love
     * qq
     * see awesome book
     * qq who love
     * please google
     * enough book love me
     * ➜  compress
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException {

        // 压缩
        // compress("/Users/user/other/tmp/MapReduce/compress/f1", "org.apache.hadoop.io.compress.GzipCodec");
        // compress("/Users/user/other/tmp/MapReduce/compress/f1", GzipCodec.class);
        // compress("/Users/user/other/tmp/MapReduce/compress/f1", BZip2Codec.class);

        // 解压
        // deCompress("/Users/user/other/tmp/MapReduce/compress/f1.gz", "org.apache.hadoop.io.compress.GzipCodec");
        // deCompress("/Users/user/other/tmp/MapReduce/compress/f1.gz", GzipCodec.class);
        // deCompress("/Users/user/other/tmp/MapReduce/compress/f1.bz2", BZip2Codec.class);
    }


    private static void compress(String fileName, String className) throws ClassNotFoundException, IOException {
        compress(fileName, Class.forName(className));
    }

    /**
     * 压缩文件, 压缩到当前目录, 压缩后的文件名自动加上后缀 (当前编解码器的默认后缀)
     *
     * @param fileName 文件名
     * @param theClass 编解码器类
     * @throws IOException
     */
    private static void compress(String fileName, Class theClass) throws IOException {

        // 获取编解码对象
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(theClass, new Configuration());
        String outputFileName = fileName + codec.getDefaultExtension();

        // 获取输入流
        FileInputStream input = new FileInputStream(new File(fileName));

        // 获取输出流
        FileOutputStream output = new FileOutputStream(new File(outputFileName));
        // 获取压缩输出流
        CompressionOutputStream compressionOutput = codec.createOutputStream(output);

        // 拷贝流 (输入流 -> 压缩输出流)
        IOUtils.copyBytes(input, compressionOutput, 1024 * 1024, false);

        // 关闭流
        IOUtils.closeStream(compressionOutput);
        IOUtils.closeStream(output);
        IOUtils.closeStream(input);
    }

    private static void deCompress(String fileName, String className) throws ClassNotFoundException, IOException {
        deCompress(fileName, Class.forName(className));
    }

    /**
     * 解压文件, 解压到当前目录, 解压后的文件名自动加上后缀 (.decode)
     *
     * @param fileName 文件名
     * @param theClass 编解码器类
     * @throws IOException
     */
    private static void deCompress(String fileName, Class theClass) throws IOException {

        // 检查压缩方式
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = compressionCodecFactory.getCodec(new Path(fileName));
        String outputFileName = fileName + ".decode";

        if (codec != null) {

            // 获取输入流
            FileInputStream input = new FileInputStream(new File(fileName));
            // 获取压缩输入流
            CompressionInputStream compressionInput = codec.createInputStream(input);

            // 获取输出流
            FileOutputStream output = new FileOutputStream(new File(outputFileName));

            // 拷贝流 (压缩输入流 -> 输出流)
            IOUtils.copyBytes(compressionInput, output, 1024 * 1024, false);

            // 关闭流
            IOUtils.closeStream(output);
            IOUtils.closeStream(compressionInput);
            IOUtils.closeStream(input);
        }
    }

}
