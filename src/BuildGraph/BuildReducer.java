package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BuildReducer extends Reducer<Text, Text, Text, OutLink>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{        

        double initial_rank = context.getConfiguration().getDouble("initial_rank", 99999);
        OutLink link = new OutLink();
        link.setRank(initial_rank);

        for(Text t: values){
            if(!t.toString().equals("***self***"))
                link.add(new Text(t));
        }

        link.setNumOutlink();
        context.write(key, link);

        //if(link.isDangling())
        //    context.getCounter("BuildReducer", "Total Dangling Node").increment(1);
        //context.getCounter("BuildReducer", "Total Page").increment(1);
    }
}
