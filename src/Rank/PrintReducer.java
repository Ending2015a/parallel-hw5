package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrintReducer extends Reducer<Text, OutLink, Text, OutLink>{

    public void reduce(Text key, Iterable<OutLink> values, Context context) throws IOException, InterruptedException{
    
        for(OutLink l: values){
        
            context.write(key, l);
        }
    }
}
