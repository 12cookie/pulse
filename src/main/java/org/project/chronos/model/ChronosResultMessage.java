package org.project.chronos.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChronosResultMessage {

    private String taskId;
    private String taskExecutorId;
    private String taskResult;
}
