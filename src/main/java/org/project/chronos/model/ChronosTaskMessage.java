package org.project.chronos.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ChronosTaskMessage implements Serializable {

    private String taskId;
    private ChronosTask chronosTask;
}
