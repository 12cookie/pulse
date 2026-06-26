package org.project.chronos.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AssignedTaskWrapper implements Serializable {

    Map.Entry<Long, AssignedTask> assignedTaskEntry;
}

