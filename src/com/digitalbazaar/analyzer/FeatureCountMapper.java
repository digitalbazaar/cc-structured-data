package com.digitalbazaar.analyzer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.commoncrawl.protocol.shared.ArcFileItem;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

/**
 * Outputs all features contained within the markup of pages contained
 * within {@code ArcFileItem} objects.
 * 
 * @author Manu Sporny <msporny@digitalbazaar.com>
 * @author Steve Salevan <steve.salevan@gmail.com> (original WordCountMapper author)
 */
public class FeatureCountMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> 
{
   public void map(Text key, ArcFileItem value,
     OutputCollector<Text, LongWritable> output, Reporter reporter)
     throws IOException 
   {
      try 
      {
         if(!value.getMimeType().contains("html")) 
         {
            return;  // Only parse text.
         }

         // HTML documents counter
         output.collect(new Text("HTML_DOCUMENTS"), new LongWritable(1));

         // Retrieves page content from the passed-in ArcFileItem.
         ByteArrayInputStream inputStream = new ByteArrayInputStream(
            value.getContent().getReadOnlyBytes(), 0,
            value.getContent().getCount());
         // Converts InputStream to a String.
         String content = new Scanner(inputStream).useDelimiter("\\A").next();

         // Create a DOM out of the HTML
         Document doc = Jsoup.parse(content);

         // Count all attributes containing a property attribute   
         Elements properties = doc.select("[property]");
         if(properties.size() > 0)
         {
            output.collect(new Text("RDFA_DOCUMENTS"), new LongWritable(1));
            output.collect(new Text("RDFA_PROPERTY"), 
               new LongWritable(properties.size()));
         }
      }
      catch(Exception e) 
      {
         reporter.getCounter("FeatureCountMapper.exception",
            e.getClass().getSimpleName()).increment(1);
      }
   }
}

