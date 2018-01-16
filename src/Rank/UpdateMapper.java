package page_rank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpdateMapper extends Mapper<Text, OutLink, Text, PageInfo>{

    public void map(Text key, OutLink value, Context context) throws IOException, InterruptedException{
    
        ArrayList<Text> link = value.getOutlink();

        for(Text l: link){
            context.write(l, new PageInfo(value.getNumOutlink(), value.getRank()));
        }

        context.write(key, new PageInfo(value));
    }
}
