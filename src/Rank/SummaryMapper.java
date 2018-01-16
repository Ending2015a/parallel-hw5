package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SummaryMapper extends Mapper<Text, OutLink, Text, DoubleWritable>{

    public void map(Text key, OutLink link, Context context) throws IOException, InterruptedException{
    
        if(link.isDangling()){
            context.write(new Text("d"), new DoubleWritable(link.getRank()));
        }

        context.write(new Text("e"), new DoubleWritable(link.getError()));
    }
}
