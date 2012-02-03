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
            return;  // Only parse HTML content
         }

         // HTML documents counter
         output.collect(new Text("HTML_DOCUMENTS"), new LongWritable(1));

         // Retrieves page content from the passed-in ArcFileItem.
         ByteArrayInputStream inputStream = new ByteArrayInputStream(
            value.getContent().getReadOnlyBytes(), 0,
            value.getContent().getCount());
         // Converts InputStream to a String.
         String data = new Scanner(inputStream).useDelimiter("\\A").next();

         // Create a DOM out of the HTML
         Document doc = Jsoup.parse(data);

         // Count all attributes containing an about attribute
         boolean rdfaDocument = false;
         Elements about = doc.select("[about]");
         if(about.size() > 0)
         {
        	rdfaDocument = true;
            output.collect(new Text("RDFA_ABOUT"), 
               new LongWritable(about.size()));
         }

         // Count all attributes containing a datatype attribute
         Elements datatype = doc.select("[datatype]");
         if(datatype.size() > 0)
         {
         	rdfaDocument = true;
        	output.collect(new Text("RDFA_DATATYPE"), 
               new LongWritable(datatype.size()));
         }
         // Count all attributes containing a prefix attribute
         Elements prefix = doc.select("[prefix]");
         if(prefix.size() > 0)
         {
         	rdfaDocument = true;
        	output.collect(new Text("RDFA_PREFIX"), 
                     new LongWritable(prefix.size()));
         }
         // Count all attributes containing a property attribute
         Elements property = doc.select("[property]");
         if(property.size() > 0)
         {
         	rdfaDocument = true;
        	output.collect(new Text("RDFA_PROPERTY"), 
                     new LongWritable(property.size()));
         }
         // Count all attributes containing a resource attribute
         Elements resource = doc.select("[resource]");
         if(resource.size() > 0)
         {
            rdfaDocument = true;
        	output.collect(new Text("RDFA_RESOURCE"), 
                     new LongWritable(resource.size()));
         }
         // Count all attributes containing a typeof attribute
         Elements typeof = doc.select("[typeof]");
         if(typeof.size() > 0)
         {
          	rdfaDocument = true;
        	output.collect(new Text("RDFA_TYPEOF"), 
                     new LongWritable(typeof.size()));
         }
         // Count all attributes containing a vocab attribute
         Elements vocab = doc.select("[vocab]");
         if(vocab.size() > 0)
         {
          	rdfaDocument = true;
          	output.collect(new Text("RDFA_VOCAB"), 
                     new LongWritable(vocab.size()));
         }
         // Count all attributes containing a inlist attribute
         Elements inlist = doc.select("[inlist]");
         if(inlist.size() > 0)
         {
          	rdfaDocument = true;
          	output.collect(new Text("RDFA_INLIST"), 
                     new LongWritable(inlist.size()));
         }
         
         if(rdfaDocument)
         {
             output.collect(new Text("RDFA_DOCUMENTS"), new LongWritable(1));

             // Count all attributes containing a content attribute
             Elements content = doc.select("[content]");
             if(content.size() > 0)
             {
            	 output.collect(new Text("RDFA_CONTENT"), 
                         new LongWritable(content.size()));
             }
         }

         // Count all attributes containing an itemscope attribute
         Elements itemscope = doc.select("[itemscope]");
         if(itemscope.size() > 0)
         {
             output.collect(new Text("MICRODATA_DOCUMENTS"), new LongWritable(1));
        	 output.collect(new Text("MICRODATA_ITEMSCOPE"), 
                     new LongWritable(itemscope.size()));
        	 
	         // Count all attributes containing a itemtype attribute
	         Elements itemtype = doc.select("[itemtype]");
	         if(itemtype.size() > 0)
	         {
	        	 output.collect(new Text("RDFA_ITEMTYPE"), 
	                     new LongWritable(itemtype.size()));
	         }
	         // Count all attributes containing a itemprop attribute
	         Elements itemprop = doc.select("[itemprop]");
	         if(itemprop.size() > 0)
	         {
	        	 output.collect(new Text("RDFA_ITEMPROP"), 
	                     new LongWritable(itemprop.size()));
	         }
	         // Count all attributes containing a itemref attribute
	         Elements itemref = doc.select("[itemref]");
	         if(itemref.size() > 0)
	         {
	        	 output.collect(new Text("RDFA_ITEMREF"), 
	                     new LongWritable(itemref.size()));
	         }
         }         

         // Count all hcard Microformats
         boolean ufDetected = false;
         Elements hcard = doc.select("[class~=hcard]");
         if(hcard.size() > 0)
         {
        	 ufDetected = true;
        	 output.collect(new Text("UF_HCARD"), 
                     new LongWritable(itemscope.size()));
         }         

         // Count all hcard Microformats
         Elements hrecipe = doc.select("[class~=hrecipe]");
         if(hrecipe.size() > 0)
         {
        	 ufDetected = true;
        	 output.collect(new Text("UF_HRECIPE"), 
                     new LongWritable(hrecipe.size()));
         }         

         // Count all hCalendar Microformats
         Elements hcalendar = doc.select("[class~=vevent]");
         if(hcalendar.size() > 0)
         {
        	 ufDetected = true;
        	 output.collect(new Text("UF_HCALENDAR"), 
                     new LongWritable(hcalendar.size()));
         }         

         // if a Microformat was detected, up the Microformat document count
         if(ufDetected)
         {
            output.collect(new Text("UF_DOCUMENTS"), new LongWritable(1));
         }
      }
      catch(Exception e) 
      {
         reporter.getCounter("FeatureCountMapper.exception",
            e.getClass().getSimpleName()).increment(1);
      }
   }
}

