package org.project.chronos.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChronosResultMessage {

    private String taskId;
    private boolean success;
    private String taskExecutorId;
    private String taskResult;
    private Map<Integer, String> errorMessage;
}
