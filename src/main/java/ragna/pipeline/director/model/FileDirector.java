package ragna.pipeline.director.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FileDirector {
    private String fileName;
    private FileStage stage = FileStage.IN_PROCESS;

    public enum FileStage {IN_PROCESS, PROCESSED, DOWNLOADED}
}
