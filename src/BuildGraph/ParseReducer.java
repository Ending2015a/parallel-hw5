package page_rank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class ParseReducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        
        //context.getCounter("ParseReducer", "Total linked page").increment(1);

        ArrayList<Text> list = new ArrayList<Text>();

        boolean isValid = false;

        for(Text t: values){
            list.add(new Text(t));
            if(t.toString().equals("***self***")){
                isValid = true;
            }
        }
        
        if(!isValid)return;

        for(Text title: list){
            context.write(key, title);
        }

        //context.getCounter("ParseReducer", "Total valid page").increment(1);
    }

}
