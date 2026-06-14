package org.project.chronos.constants;

import lombok.Getter;

@Getter
public enum ProcessingStatus {

    TASK_PENDING("TASK_PENDING"),

    TASK_COMPLETED("TASK_COMPLETED"),;

    private final String description;

    ProcessingStatus(String description) {
        this.description = description;
    }
}
