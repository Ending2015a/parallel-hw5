package page_rank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;


public class Rank{

    private String rankTmpPath = "Rank/";

    public ArrayList<Double> errorList;

    public Rank(String root){
    
        rankTmpPath = root + "/" + rankTmpPath;
        errorList = new ArrayList<Double>();
    }

    public String getTmpPath(String str, int i){
        return String.format("%s%s/tmp-%05d", rankTmpPath, str, i);
    }

    public void deleteFolder(String folder) throws Exception{
        Configuration conf = new Configuration();

        Path fd = new Path(folder);
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(fd)){
            fs.delete(fd, true);
        }
    }

    public String exe(String in, int numReducer, long total_page, int total_iterate) throws Exception{
    
        String update_out = "";
        String summary_out = getTmpPath("summary", 0);

        this.jobSummary(in, summary_out);
        double summary[] = readSummary(summary_out + "/part-r-00000");

        if(total_iterate == -1){
            total_iterate = 0;
            while(true){

                System.out.println(String.format("Iteration: %d", total_iterate+1));
                // update page rank
                update_out = getTmpPath("update", total_iterate);
                this.jobUpdate(in, update_out, numReducer, total_page, summary[1]);

                deleteFolder(in);

                // increase iteration time
                total_iterate++;

                // write summary
                summary_out = getTmpPath("summary", total_iterate);
                this.jobSummary(update_out, summary_out);
                summary = readSummary(summary_out + "/part-r-00000");

                System.out.println(String.format("Error: %.10f, Dang: %.10f", summary[0], summary[1]));
                
                errorList.add(summary[0]);

                // update file path
                in = update_out;

                if(summary[0] < 0.001)
                    break;
            }
        }else{
            int i=0;
            while(i<total_iterate){

                System.out.println(String.format("Iteration: %d", i+1));

                update_out = getTmpPath("update", i);
                this.jobUpdate(in, update_out, numReducer, total_page, summary[1]);

                ++i;

                summary_out = getTmpPath("summary", i);
                this.jobSummary(update_out, summary_out);

                summary = readSummary(summary_out + "/part-r-00000");

                System.out.println(String.format("Error: %.10f, Dang: %.10f", summary[0], summary[1]));

                errorList.add(summary[0]);

                // update file path
                in = update_out;
            }
        }



        return in;
    }

    public double[] readBinarySummary(String in) throws Exception{
        Configuration conf = new Configuration();
        Option fileOption = SequenceFile.Reader.file( new Path(in) );
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);
        
        Text key = new Text();
        DoubleWritable value = new DoubleWritable();

        double err = 0;
        double dang = 0;

        while(reader.next(key, value)){
            if(key.toString().equals("d"))
                dang = value.get();
            else if(key.toString().equals("e"))
                err = value.get();
        }

        reader.close();

        return new double[] {err, dang};
    }

    public double[] readSummary(String in) throws Exception{
        double error = 0.0;
        double dang_rank = 0.0;
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open( new Path(in) )));
        String line;
        line = br.readLine();

        while(line != null){
            String[] str = line.split("\t");
            if(str[0].equals("d"))
                dang_rank = Double.parseDouble(str[1]);
            else if(str[0].equals("e"))
                error = Double.parseDouble(str[1]);

            line = br.readLine();
        }

        return new double[] {error, dang_rank};

    }

    private void jobSummary(String in, String out) throws Exception{
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Summary");
        job.setJarByClass(Rank.class);

        //set Mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SummaryMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        //set Partitioner

        //set Reducer
        job.setReducerClass(SummaryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }

    private void jobUpdate(String in, String out, int numReducer, long total_page, double dang_rank) throws Exception{
    
        Configuration conf = new Configuration();

        conf.setLong("total_page", total_page);
        conf.setDouble("dang_rank", dang_rank);

        Job job = Job.getInstance(conf, "Update");
        job.setJarByClass(Rank.class);

        //set Mapper
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(UpdateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageInfo.class);

        //set Partitioner
        job.setPartitionerClass(UpdatePartitioner.class);

        //set Reducer
        job.setReducerClass(UpdateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OutLink.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(numReducer);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }

    private void jobPrint(String in, String out) throws Exception{
    
        Job job = Job.getInstance(new Configuration(), "Print");
        job.setJarByClass(Rank.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(PrintMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OutLink.class);

        job.setReducerClass(PrintReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OutLink.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }
}

