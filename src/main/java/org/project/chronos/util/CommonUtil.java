package org.project.chronos.util;

import grpc.chronos.executor.ChronosTask;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.project.chronos.model.ChronosTaskMessage;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

@Slf4j
public class CommonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    public static <T> T mapStringToTypeReference(String json, TypeReference<T> typeReference) {
        return objectMapper.readValue(json, typeReference);
    }

    @SneakyThrows
    public static <T> T mapStringToObject(String objString, Class<T> classType) {
        return objectMapper.readValue(objString, classType);
    }

    @SneakyThrows
    public static String mapObjectToString(Object object) {
        return objectMapper.writeValueAsString(object);
    }

    public static grpc.chronos.executor.ChronosTask createScrapingTaskRequest(ChronosTaskMessage task) {
        return ChronosTask.newBuilder()
                .setTaskAvailable(true)
                .setTaskId(task.getTaskId())
                .setTaskData(task.getChronosTask().getTaskMetadata())
                .build();
    }

}
