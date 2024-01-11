import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

public class InputFileData {

    private final Path filepath;
    private final int numHeaderColumns;
    private final boolean isValidFile;
    private  String[] columnHeaders;

    public InputFileData(Path filepath, int numHeaderColumns, boolean isValidFile) {
        this.filepath = filepath;
        this.numHeaderColumns = numHeaderColumns;
        this.isValidFile = isValidFile;
        
    }

    public Path getFilepath() {
        return filepath;
    }

    public int getNumHeaderColumns() {
        return numHeaderColumns;
    }

    public boolean isValidFile() {
        return isValidFile;
    }
    public String[] getFileHeaders()
    {
    	return this.columnHeaders;
    }
    public Stream<String> readFileLines()
    {
    	try {
            // Implement your file reading logic here
            Stream<String>lines = Files.lines(this.filepath, StandardCharsets.UTF_8);
            return lines;
        } catch (IOException e) {
            throw new RuntimeException("Error reading file " + filepath.toFile().getName(), e);
            
        }
    }
    public String[] readHeaderLine()
    {
    		Optional<String> header = this.readFileLines().findFirst();
    		
			return this.columnHeaders = header.get().split("\\|");
    }
}

