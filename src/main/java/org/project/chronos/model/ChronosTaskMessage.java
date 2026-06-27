package org.project.chronos.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
public class ChronosTaskMessage implements Serializable {

    @JsonCreator
    public ChronosTaskMessage() {
        this.errorMessage = new HashMap<>();
        this.resubmissionCount = 0;
    }

    @JsonProperty(required = true)
    private String taskId;

    @JsonProperty(required = true)
    private ChronosTask chronosTask;

    private Map<Integer, String> errorMessage;
    private int resubmissionCount;
}
