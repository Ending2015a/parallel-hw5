package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrintMapper extends Mapper<Text, OutLink, Text, OutLink>{

    public void map(Text key, OutLink link, Context context) throws IOException, InterruptedException{
    
        context.write(key, link);
    }
}
