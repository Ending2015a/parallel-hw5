package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;

public class BuildGraph{

    private String buildGraphTmpPath = "BuildGraph/tmp";


    public BuildGraph(String root){
        buildGraphTmpPath = root + "/" + buildGraphTmpPath;
    }


    public String getTmpPath(int i){
        return String.format("%s-%03d", buildGraphTmpPath, i);
    }

    public void deleteFolder(String folder) throws Exception{
        Configuration conf = new Configuration();

        Path fd = new Path(folder);
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(fd)){
            fs.delete(fd, true);
        }
    }

    public long exe(String in, String out, int numReducer) throws Exception{
    
        long total_page = this.jobParse(in, getTmpPath(0), numReducer);
        double inv_total = 1.0/(double)total_page;

        System.out.println(String.format("total page: %d, initial_rank: %f", total_page, inv_total));

        this.jobBuild(getTmpPath(0), out, numReducer, inv_total);
        //long result[] = this.jobCount(out, getTmpPath(1), 1);

        deleteFolder(getTmpPath(0));

        return total_page;
    }

    private long jobParse(String in, String out, int numReducer) throws Exception{
        Job job = Job.getInstance(new Configuration(), "ParseInput");
        job.setJarByClass(BuildGraph.class);
        
        //set Mapper
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //set Partitioner
        job.setPartitionerClass(ParsePartitioner.class);

        //set Reducer
        job.setReducerClass(ParseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(numReducer);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);

        return job.getCounters().findCounter("ParseMapper", "Total Page").getValue();
    }

    private void jobBuild(String in, String out, int numReducer, double initial_rank) throws Exception{
        Configuration conf = new Configuration();
        conf.setDouble("initial_rank", initial_rank);
        Job job = Job.getInstance(conf, "BuildGraph");
        job.setJarByClass(BuildGraph.class);

        //set Mapper
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(BuildMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //set Partitioner
        job.setPartitionerClass(BuildPartitioner.class);

        //set Reducer
        job.setOutputFormatClass(SequenceFileOutputFormat.class); 
        job.setReducerClass(BuildReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OutLink.class);

        job.setNumReduceTasks(numReducer);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }

    private long[] jobCount(String in, String out, int numReducer) throws Exception{
        Job job = Job.getInstance(new Configuration(), "CountNode");
        job.setJarByClass(BuildGraph.class);

        //set Mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);

        long count = job.getCounters().findCounter("CountNode", "Count").getValue();
        long dang = job.getCounters().findCounter("CountNode", "Dang").getValue();

        return new long[] {count, dang};
    }
}
