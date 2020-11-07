import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class least_5_mapper extends Mapper<Object,Text,Text,LongWritable> {

	private HashMap<String,Long> tmap;
	
	
	@Override
	public void setup(Context context)throws IOException,InterruptedException
	{
		tmap = new HashMap<String,Long>();
		
	}
	
	
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{	
		     //input data format => movie_name    no_of_views  (tab seperated)
		
		
				//we split the input data 
				String [] input = value.toString().split(" ");
				
				//String movie_name = tokens[0];
				//long no_of_views = Long.parseLong(tokens[1]);
				
                for(int f = 0; f < input.length; f++) {         
                    String word = input[f];
                    if(input[f].length() > 1) {
                        if(tmap.get(word) == null) {
                            tmap.put(word, new Long(1));
                            }   
                        else {
                            tmap.put(word, (tmap.get(word))+1);
                        }
                    }       
                }       
				//insert data into treeMap , we want top 10  viewed movies
				//so we pass no_of_views as key
				//tmap.put(no_of_views, movie_name);   
			   	/*			
				if(tmap.size()>5)
				{
                    while(tmap.size()>5){
                        tmap.remove(tmap.lastKey());
                    }
                }
                */
                //tmap = sortByValues(tmap);
		
		
    }
    
    
    private static HashMap sortByValues(HashMap map) { 
        List list = new LinkedList(map.entrySet());
        // Defined Custom Comparator here
        Collections.sort(list, new Comparator() {
                public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                    .compareTo(((Map.Entry) (o2)).getValue());
                }
        });

        // Here I am copying the sorted list in HashMap
        // using LinkedHashMap to preserve the insertion order
        HashMap sortedHashMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                sortedHashMap.put(entry.getKey(), entry.getValue());
        } 
        return sortedHashMap;
    }
	
	@Override
	public void cleanup(Context context)throws IOException,InterruptedException
	{
		for(Map.Entry<String,Long> entry : tmap.entrySet()) {
			  
			  String word = entry.getKey();         
			  Long count = entry.getValue();

			  context.write(new Text(word),new LongWritable(count));
			  
			}
		
	}
	
}