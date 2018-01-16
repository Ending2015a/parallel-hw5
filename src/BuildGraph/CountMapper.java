package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class CountMapper extends Mapper<Text, OutLink, Text, IntWritable> {

    public void map(Text key, OutLink value, Context context) throws IOException, InterruptedException{
          
        context.write(new Text("Count"), new IntWritable(1));

        if(value.isDangling()){
            context.write(new Text("Dang"), new IntWritable(1));
        }

    }
}
