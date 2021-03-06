package page_rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UpdateReducer extends Reducer<Text, PageInfo, Text, OutLink>{

    private double alpha = 0.85;

    public void reduce(Text key, Iterable<PageInfo> values, Context context) throws IOException, InterruptedException{
    
        long total_page = context.getConfiguration().getLong("total_page", 10);
        double inv = 1.0/(double)total_page;
        double dang_rank = context.getConfiguration().getDouble("dang_rank", 0.0);

        double pred = 0.0;

        OutLink outlink = new OutLink();

        for(PageInfo p: values){
            if(p.isOutlink()){
                outlink = new OutLink(p.getOutlink());
                //context.getCounter("UpdateReducer", "getOutlink").increment(1);
                continue;
            }

            pred += p.getRank();
        }

        double new_rank = (1.0-alpha)*inv + alpha * pred + alpha * dang_rank * inv;

        outlink.newRank(new_rank);

        context.write(key, outlink);

        //context.getCounter("UpdateReducer", "total_page").setValue(total_page);
    }
}
