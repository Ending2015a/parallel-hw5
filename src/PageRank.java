package page_rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank{
    public static void main(String[] args) throws Exception{
    
        int numIterate = Integer.parseInt(args[3]);
        int numReducer = Integer.parseInt(args[4]);

        //job1
        BuildGraph job1 = new BuildGraph(args[0]);
        long total_page = job1.exe(args[1], "PageRank/BuildGraph/Output", numReducer);

        System.out.println(String.format("count: %d", total_page));
        //job2
        Rank job2 = new Rank(args[0]);
        String output = job2.exe("PageRank/BuildGraph/Output", numReducer, total_page, numIterate);

        System.out.println("job output path: " + output);

        //job3
        Result job3 = new Result();
        job3.exe(output, args[2], 1);

            System.out.println(String.format("Iteration\t\tError"));
        for(int i=0;i<job2.errorList.size();++i){
            System.out.println(String.format("%9d\t\t%.20f", i+1, job2.errorList.get(i)));
        }
    
        System.exit(0);
    }
}
