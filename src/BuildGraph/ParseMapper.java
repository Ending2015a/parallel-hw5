package page_rank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.net.URI; 
import java.io.*;



public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        Text title;
        Text link;
         	
		/*  Match title pattern */  
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(value.toString());
        if(titleMatcher.find()){
            title = new Text(
                        unescapeXML(
                                titleMatcher.group().replaceAll("\\<.*?\\>", "")
                            )
                    );
        }else{
            return;
        }
		// No need capitalizeFirstLetter
		
		/*  Match link pattern */
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher(value.toString());


        while(linkMatcher.find()){
            link = new Text( 
                            capitalizeFirstLetter(
                                    unescapeXML(linkMatcher.group().replaceAll("\\[\\[(.+?)([\\|#]|\\]\\])", "$1"))
                                )
                            );
            context.write(link, title);
        }
		// Need capitalizeFirstLetter

        // self link
        context.write(title, new Text("***self***"));

        context.getCounter("ParseMapper", "Total Page").increment(1);
	}
	
	private String unescapeXML(String input) {

		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");

    }

    private String capitalizeFirstLetter(String input){

    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
    }
}
