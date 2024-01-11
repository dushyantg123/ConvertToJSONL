import java.nio.file.Path;

public class InputFileToProcess {

    private final Path filepath;
    private final int numHeaderColumns;
    private final boolean isValidFile;
    private final String inputFileProcessingStatus;

    public InputFileToProcess(Path filepath, int numHeaderColumns, boolean isValidFile, String inputFileProcessingStatus) {
        this.filepath = filepath;
        this.numHeaderColumns = numHeaderColumns;
        this.isValidFile = isValidFile;
        this.inputFileProcessingStatus = inputFileProcessingStatus;
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
    public String getInputFileProcessingStatus()
    {
    	return this.inputFileProcessingStatus;
    }
}

