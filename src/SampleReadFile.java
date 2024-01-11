import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.IOException;

public class SampleReadFile {

	    public static void main(String[] args) {
	        String filePath = "c:\\test\\input.csv"; // Replace with the actual path to your file

	       /* try {
	        		Path path = Paths.get(filePath);
	        		long size = Files.size(path);
	        		if (size == 0) {
	        			System.out.println("The file is empty.");
	        		}
	        		else
	        		{
	        			Stream<String> lines = Files.lines(path);
	        			
	        			
	        			Stream<String[]> splitLine = lines.map(line -> { String s[] = line.split("|"); return (s);});
	        			Iterator<String[]> iterator = splitLine.iterator();
	        			
	        			
	        		}
	        }
	        		catch (IOException e) {
	        				e.printStackTrace(); // Handle IOException appropriately, e.g., log or throw
	        			}
	        			*/
	      
	        try {
	            Path path = Paths.get(filePath);

	            // Read all lines from the file
	    //        List<String> lines = Files.lines(path).collect(Collectors.toList());

	            // Check if the file is empty
	            long size = Files.size(path);
        		if (size == 0) {
        			System.out.println("The file is empty.");
        			return;
        		}
    			Stream<String> lines = Files.lines(path);
	            // Extract column headers from the first line
    			Optional<String> headerLine = lines.findFirst();
    			String[] columnHeaders = headerLine.get().split("\\|");
	            lines = Files.lines(path);	
	            // Process each line and print column values
	            lines
	                    .skip(1) // Skip the first line (header)
	                    .forEach(line -> {
	                        List<String> columnValues = Arrays.asList(line.split("\\|"));
	                        System.out.print("{");
	                        for (int i = 0; i < columnHeaders.length; i++) {
	                            String columnHeader = columnHeaders[i];
	                            String columnValue = columnValues.get(i);
	                            
	                            writeCode(columnHeader,columnValue);
	                            if (i-1 != columnHeaders.length)
	                            System.out.println(",");
	                        }
	                        System.out.print("}");
	                        System.out.println("--------------------");
	                    });

	        } catch (IOException e) {
	            e.printStackTrace(); // Handle IOException appropriately, e.g., log or throw
	        }
	    }

		private static void writeCode(String columnHeader, String columnValue) {
			System.out.print("\"" + columnHeader + "\":" + "\"" + columnValue + "\"");
			
		}
	        	}
	    
	
