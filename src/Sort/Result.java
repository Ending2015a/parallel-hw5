package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Result{

    public Result(){
    
    }

    public void exe(String in, String out, int numReducer) throws Exception{
        Job job = Job.getInstance(new Configuration(), "Result");
        job.setJarByClass(Result.class);

        //set Mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(ResultMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        //set Partitioner
        //job.setPartitionerClass()
        job.setSortComparatorClass(ResultComparator.class);
        
        //set Reducer
        job.setReducerClass(ResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setNumReduceTasks(numReducer);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }
}
