package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BuildMapper extends Mapper<Text, Text, Text, Text>{

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

        if(value.toString().equals("***self***")){
            context.write(key, new Text("***self***"));
        }else{
            context.write(value, key);
        }
    }
}
