package org.project.chronos.grpc;

import grpc.chronos.executor.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.project.chronos.service.ChronosTaskManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
public class ChronosTaskExecutorService extends ChronosTaskExecutorServiceGrpc.ChronosTaskExecutorServiceImplBase {

    @Autowired
    private ChronosTaskManager taskManager;

    @Override
    public void getTask(GetTaskRequest request, StreamObserver<ChronosTask> responseObserver) {
        responseObserver.onNext(taskManager.getChronosTask(request));
        responseObserver.onCompleted();
    }

    @Override
    public void submitTaskResult(ResultSubmissionRequest request, StreamObserver<ResultAcknowledgment> responseObserver) {
        responseObserver.onNext(taskManager.submitTaskResult(request));
        responseObserver.onCompleted();
    }

}
