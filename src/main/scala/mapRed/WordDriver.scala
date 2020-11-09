package wordCount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, Writable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}

/**
 * WordDriver is the eentry point for our MapReduce job.
 * In this class we configure input and output data formats, configure Mapper
 * and Reducer classes, and specify intermediate data formats
 *
 * We're going to make use of command line arguments here which can be accessed
 * in the args array
 * */
object WordDriver extends App {

  if (args.length != 2) {
    println("Usage: WordDriver <input dir> <output dir>")
    System.exit(-1)
  }

  //  val conf : Configuration = new Configuration();

  val job1 = Job.getInstance()
  job1.setJobName("word count")
  job1.setInputFormatClass(classOf[TextInputFormat])

  FileInputFormat.addInputPath(job1, new Path(args(0)))
  FileOutputFormat.setOutputPath(job1, new Path(args(1)))

  job1.setJarByClass(WordDriver.getClass)

  job1.setMapperClass(classOf[AU_Mapper])
  //  job1.setCombinerClass(classOf[WordReducer])
  job1.setReducerClass(classOf[WordReducer])

  job1.setOutputKeyClass(classOf[Text])
  job1.setOutputValueClass(classOf[IntWritable])
  //  job1.setOutputFormatClass(classOf[SequenceFileOutputFormat[Writable, Writable]])

  val success = job1.waitForCompletion(true)
  System.exit(if (success) 0 else 1)

  //  val job2 = Job.getInstance(conf, "sort by frequency")
  //  job2.setJarByClass(WordDriver.getClass)
  //  job2.setMapperClass(classOf[KeyValueSwappingMapper])
  ////  job2.setNumReduceTasks(1)
  ////  job2.setSortComparatorClass(classOf[IntWritable.DecreasingComparator])
  //  job2.setOutputKeyClass(classOf[IntWritable])
  //  job2.setOutputValueClass(classOf[Text])
  //  job2.setInputFormatClass(classOf[SequenceFileInputFormat[Writable, Writable]])
  //
  //  FileInputFormat.addInputPath(job2, new Path(args(1)))
  //  FileOutputFormat.setOutputPath(job2, new Path(args(2)))
  //
  //  if (!job2.waitForCompletion(true)) {
  //    System.exit(1)
  //  }
}
