package org.project.chronos.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AssignedTask {

    private String taskExecutorClientId;
    private ChronosTaskMessage task;

}
