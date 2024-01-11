import java.io.File;
import org.apache.commons.cli.*;

import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class CreateJSONLFromCSV {

	
	private static String inputpath = "";
	private static String outputpath="";
	private static String filters = ".csv";
	public static void main(String[] args) {
	
		try {
			inputpath =  new java.io.File(".").getCanonicalPath();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		outputpath = inputpath + "\\output";
		checkinputParams(args);
		ArrayList<File> filesToProcess = applyFilter();
		/*
		 *1.  Validate the file first - header is present and then each line has same number of text strings
		 *2. If validation fails - add the file to invalid file list
		 *3. open a new output jsonl file
		 *4.  read input line and write lines to output file
		 *5.  add the input file to processed file list
		 */ 
		
		
		
		ExecutorService executorService = Executors.newFixedThreadPool(Math.min(100, Runtime.getRuntime().availableProcessors() * 2));
	//	ExecutorService executorService = Executors.newFixedThreadPool(20);
		processFileAsync(filesToProcess,executorService);
		
	//	filesToProcess.stream().forEach(fileName -> processFileAsync(fileName, executorService));

		/*
        // Shutdown the executor service when done
		try {
            // Optionally wait for tasks to complete or a timeout
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                // Handle the case where tasks didn't complete within the timeout
                System.out.println("ExecutorService did not terminate within the timeout.");
            }
        } catch (InterruptedException e) {
            // Handle InterruptedException
            e.printStackTrace();
        }*/
		System.out.println("main thread completed. Now shutting down executor Service");
		executorService.shutdown();
		System.out.println("main thread completed. executor Service is shutdown now");
	}
		

	 private static void processFileAsync(ArrayList<File> filesToProcess, ExecutorService executorService) {
		 
		 List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
		 for( File filename : filesToProcess)
		 {
			 CompletableFuture<Void> taskcf  = CompletableFuture.supplyAsync(()->validateFile(filename),executorService).exceptionally(exception ->new InputFileToProcess(filename.toPath(),0,false, "The input file threw an exception error"))
				 .thenApplyAsync((InputFileToProcess input_fileToProcess) -> {
					 System.out.println("Entering ApplyAsync for filename:" + input_fileToProcess.getFilepath() + "with threadname[" + Thread.currentThread().getName() + "]");	
	                if (input_fileToProcess.isValidFile()) {
	                	//
	                	 // 	filepath,numHeaderColumns, isValidFile,inputFileProcessingStatus
	                	 //
	                	System.out.println("Exiting ApplyAsync for filename:" + input_fileToProcess.getFilepath() + "with threadname[" + Thread.currentThread().getName() + "]");
	                    return new InputFileData(input_fileToProcess.getFilepath(),input_fileToProcess.getNumHeaderColumns(),input_fileToProcess.isValidFile());
	                } else {
	                    throw new RuntimeException("File validation failed: " + filename);
	                }
	            })
	            .thenAcceptAsync((InputFileData input_filedata)->
	            {
	            	writeToFile(input_filedata);
	            })
	            .exceptionally(ex -> {
	                System.err.println("Error processing file " + filename + ": " + ex.getMessage());
	                return null; // Handle the exception gracefully, e.g., log it
	            });
	 completableFutureList.add(taskcf);
		 // Wait for all CompletableFutures to complete
	        CompletableFuture<Void> allOf = CompletableFuture.allOf(
	                completableFutureList.toArray(new CompletableFuture[0]));

	        try {
	            // Wait for all CompletableFutures to complete
	            allOf.get();
	            System.out.println("All tasks are completed.");
	        } catch (InterruptedException | ExecutionException e) {
	            e.printStackTrace();
	        }
	        System.out.println("Exiting Process File Async");
		 }	
	           
	    }
	
	 private static InputFileToProcess validateFile(File filename) {
	        
     	
		 System.out.println("Entering validateFile - File to process are: " + filename.getName());
			int columnCount = 0;
				Path filepath = filename.toPath();
				//Path filepath, int numHeaderColumns, boolean isValidFile
				try {
						Optional<String> findFirst = Files.lines(filepath, StandardCharsets.UTF_8).findFirst();
						
						if(findFirst.isEmpty())
						{
							InputFileToProcess input_ftp = new InputFileToProcess(filepath,0,false, "The file is empty");
							return input_ftp;
						}
						else
						{	
						
							System.out.println("File is not empty. The file contains data");
							if (findFirst.isPresent())
							{
								String columnHeader = findFirst.get();
								String[] columnHeaders = columnHeader.split("\\|");
								columnCount = columnHeaders.length;
								System.out.println("Number of columns:" +columnCount );
								
							}
 							else
							{
								System.out.println("Header line is missing");
								columnCount = 0;
							}	
 							if (columnCount == 0) 
							{
								return new InputFileToProcess(filepath,0,false, "The file has no columns in the file's header.");
								
							}
 							else
 							{	
							InputFileToProcess input_ftp = new InputFileToProcess(filepath,columnCount,true, "File has content and has " + columnCount + " column headers");
							System.out.println("Exiting validateFile. Valid Filename is:" + filename + " " + input_ftp.getNumHeaderColumns());
							return input_ftp;
 							}
						}		
	 
	 					} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							InputFileToProcess input_ftp = new InputFileToProcess(filepath,0,false, "The file has caught an IO error");
							return input_ftp;
						}
						catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							InputFileToProcess input_ftp = new InputFileToProcess(filepath,0,false, "The file has caught an error");
							return input_ftp;
						}
      	
     }
    
	    

	
		
	
	
	private static void writeToFile(InputFileData input_filedata) {
		System.out.println("Entering writeToFile [" + Thread.currentThread().getName() + "]");
		Stream<String> lines=null;
		System.out.println("Number of columns in the header:" + input_filedata.getNumHeaderColumns());
		 Path filePath = input_filedata.getFilepath();
		System.out.println("File path:" + filePath);
		String jsonlOutputFileName = getJSONLOutputFileName(filePath);
		Path outputFilePath = Paths.get(outputpath, jsonlOutputFileName);
		// Extract column headers from the first line
		String[] columnHeaders = input_filedata.readHeaderLine();
		lines = input_filedata.readFileLines();
		try {
    		Files.createDirectories(outputFilePath.getParent());
    		
		} 
    		catch (IOException e) {	e.printStackTrace(); }
        // Process each line and print column values
        lines
                .skip(1) // Skip the first line (header)
                .forEach(line -> 
                {
                    List<String> columnValues = Arrays.asList(line.split("\\|"));
                    StringBuffer sb = new StringBuffer(60);
                    if (columnValues.size() == columnHeaders.length)
                    {
                        sb.append("{");
                    	for (int i = 0; i < columnHeaders.length; i++) {
                        String columnHeader = columnHeaders[i];
                        String columnValue = columnValues.get(i);
                        sb.append(writeCode(columnHeader,columnValue));
                        if (i+1 != columnHeaders.length)
                        	sb.append(",");
                    }
                    sb.append("}");
                    try {
                    		Files.write(outputFilePath, (sb + System.lineSeparator()).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    		
						} 
                    		catch (IOException e) {	e.printStackTrace(); }
                    }
                }	);
        System.out.println("Exiting writeToFile [" + Thread.currentThread().getName() + "]");
		
		
	}
	private static String writeCode(String columnHeader, String columnValue) {
		String s = "\"" + columnHeader + "\":" + "\"" + columnValue + "\"";
		return s;
	}
	private static String getJSONLOutputFileName(Path path) {
		String filename = path.getFileName().toString();
		String outputFile = filename.substring(0,filename.lastIndexOf(".")) +".jsonl";
		 return outputFile;
	}

	private static void checkinputParams(String[] args) {
		CommandLineParser parser = new DefaultParser();
		// Create an options object
		        Options options = new Options();
		        //addOption(String opt, String longOpt, boolean hasArg, String description)
		        options.addOption("h", "help", false, "Display help message");
		        options.addOption("i", "inputpath", true, "Specify the input file(s) path. If input path is not provided, current file path is assumed");
		        options.addOption("o", "outputpath", true, "Specify output file path where converted .jsonl files will be stored. If output path is not provided, the output is stored in the output subfolder created under the current folder path");
		        options.addOption("f", "filter", true, "Specify comma separated list of file filter to pick the list of files from the inputpath folder");


		        try {
		            // Parse the command line arguments
		            CommandLine cmd = parser.parse(options, args);

		            // Check for the presence of optional parameters
		            if (cmd.hasOption("h")) {
		                displayHelp();
		            }
		            if (cmd.hasOption("i")) {
		            	
		            	String arg_inputpath = cmd.getOptionValue("i");
		            	processInputPath(arg_inputpath);
		            }
		            else
		            	processInputPath(inputpath);
		            
		            if (cmd.hasOption("o")) {
		            	String arg_outputpath   = cmd.getOptionValue("o");
		                processOutputFile(arg_outputpath);
		            }
		            else
		            	processOutputFile(outputpath);

		            if (cmd.hasOption("f")) {
		                String arg_filters = cmd.getOptionValue("f");
		               processFilters(arg_filters);
		            }
		            else
		            	processFilters(filters);
		            // Process other non-option arguments if needed
		            String[] remainingArgs = cmd.getArgs();
		            for (String arg : remainingArgs) {
		                processOtherArgument(arg);
		            }
		        } catch (ParseException e) {
		            System.err.println("Error parsing command line: " + e.getMessage());
		            displayHelp();
		        }
		    }

		    private static void processInputPath(String i) {
		    	inputpath = i;   
		    	System.out.println("Input argument - Input folder to read files from: " + inputpath);
		    	outputpath = inputpath + "\\output";   	
		    }
		    private static void processOutputFile(String o) {
		    	outputpath = o;
		        System.out.println("Input argument - Output folder to write JSONL files to: " + outputpath);
		        
		    }
		    private static void processFilters(String f) {
		    	filters = f;   
		    	System.out.println("Input argument - apply processing on following file extensions: " + filters);
		    	   	
			        // Process the output file as needed
		
		    	}
			private static void displayHelp() {
		        System.out.println(" -h | --help -provide details about usage of this program");
		        System.out.println(" -i | --inputpath <input path> Specify the input file(s) path. If input path is not provided, current file path is assumed");
		        System.out.println(" -o | --outputpath <output path> Specify the output file(s) path. If output path is not provided, <current input file path>\\output is used to write the converted .jsonl files");
		        System.out.println(" -f | --filter <comma separated file extensions *.csv,*.pdf> Specify comma separated list of file filter to pick the list of files from the inputpath folder");
		        System.exit(0);
		    }

		    


		    private static void processOtherArgument(String argument) {
		        System.out.println("Other argument: " + argument);
		        // Process other arguments as needed
		    }
		

	

	private static ArrayList<File> applyFilter() {
		
		String[] filter_list = filters.split(",");
		File inputDir = new File(inputpath);
		FileFilter ff;
		File[] files;
		ArrayList<File> filesArray = new ArrayList<File>();
		if(inputDir.exists() && inputDir.isDirectory())
		{
			System.out.println("applyFilter method - input path exists and is a directory");
			File[] listFiles = inputDir.listFiles();
			
			for (String filter : filter_list)
			{
				System.out.println("applyFilter method - Applying filter[" + filter + "].");
				ff = (File name) -> (name.getName()).endsWith(filter);
				files = inputDir.listFiles(ff);
				System.out.println("applyFilter method - Total files identified to be processed to JSONL conversion:"+files.length);
				if (files!=null)
					Collections.addAll(filesArray, files);
				System.out.println("applyFilter method - List of files to be processed are:" + filesArray.toString());
			}
			
			return filesArray;
		}	
		else
		{
			System.out.println("applyFilter method - Input directory does not exist or is not a directory...");
			return filesArray;
	}

	}
}

