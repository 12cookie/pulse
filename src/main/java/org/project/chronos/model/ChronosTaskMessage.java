package org.project.chronos.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ChronosTaskMessage implements Serializable {

    @JsonProperty(required = true)
    private String taskId;

    @JsonProperty(required = true)
    private ChronosTask chronosTask;

}
