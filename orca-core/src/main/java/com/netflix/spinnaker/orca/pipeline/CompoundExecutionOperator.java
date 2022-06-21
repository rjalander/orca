/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.pipeline;

import com.netflix.spinnaker.kork.core.RetrySupport;
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionType;
import com.netflix.spinnaker.orca.api.pipeline.models.PipelineExecution;
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution;
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository;
import com.netflix.spinnaker.security.AuthenticatedRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
@NonFinal
public class CompoundExecutionOperator {
  ExecutionRepository repository;
  ExecutionRunner runner;
  RetrySupport retrySupport;

  public void cancel(ExecutionType executionType, String executionId) {
    cancel(
        executionType,
        executionId,
        AuthenticatedRequest.getSpinnakerUser().orElse("anonymous"),
        null);
  }

  public void cancel(ExecutionType executionType, String executionId, String user, String reason) {
    doInternal(
        (PipelineExecution execution) -> runner.cancel(execution, user, reason),
        () -> repository.cancel(executionType, executionId, user, reason),
        "cancel",
        executionType,
        executionId);
  }

  public void delete(ExecutionType executionType, String executionId) {
    repository.delete(executionType, executionId);
  }

  public void pause(
      @Nonnull ExecutionType executionType,
      @Nonnull String executionId,
      @Nullable String pausedBy) {
    doInternal(
        runner::reschedule,
        () -> repository.pause(executionType, executionId, pausedBy),
        "pause",
        executionType,
        executionId);
  }

  public void resume(
      @Nonnull ExecutionType executionType,
      @Nonnull String executionId,
      @Nullable String user,
      @Nonnull Boolean ignoreCurrentStatus) {
    doInternal(
        runner::unpause,
        () -> repository.resume(executionType, executionId, user, ignoreCurrentStatus),
        "resume",
        executionType,
        executionId);
  }

  public PipelineExecution updateStage(
      @Nonnull ExecutionType executionType,
      @Nonnull String executionId,
      @Nonnull String stageId,
      @Nonnull Consumer<StageExecution> stageUpdater) {
    return doInternal(
        runner::reschedule,
        () -> {
          PipelineExecution execution = repository.retrieve(executionType, executionId);
          StageExecution stage = execution.stageById(stageId);
          log.info("RJR updateStage with context - {}", stage.getContext());

          // mutates stage in place
          stageUpdater.accept(stage);

          repository.storeStage(stage);
        },
        "reschedule",
        executionType,
        executionId);
  }

  public PipelineExecution restartStage(@Nonnull String executionId, @Nonnull String stageId) {
    PipelineExecution execution = repository.retrieve(ExecutionType.PIPELINE, executionId);
    log.info("RJR -->  restartStage with execution {} ", execution.getContext());
    if (repository.handlesPartition(execution.getPartition())) {
      log.info(
          "RJR --> repository handlesPartition restart PipelineExecution executionId {} stageId {} ",
          executionId,
          stageId);
      runner.restart(execution, stageId);
    } else {
      log.info(
          "Not pushing queue message action='restart' for execution with foreign partition='{}'",
          execution.getPartition());
      repository.restartStage(executionId, stageId);
    }
    return execution;
  }

  public PipelineExecution restartStage(
      @Nonnull String executionId, @Nonnull String stageId, Map restartDetails) {
    PipelineExecution execution = repository.retrieve(ExecutionType.PIPELINE, executionId);
    log.info("RJR -->  restartStage with execution {} ", execution.getContext());
    updatePreconditionStageExpression(restartDetails, execution);

    for (StageExecution stage : execution.getStages()) {
      log.info("RJR -->  restartStage with stage {} ", stage.getContext());
    }

    if (repository.handlesPartition(execution.getPartition())) {
      log.info(
          "RJR --> repository handlesPartition restart PipelineExecution executionId {} stageId {} ",
          executionId,
          stageId);
      runner.restart(execution, stageId);
    } else {
      log.info(
          "Not pushing queue message action='restart' for execution with foreign partition='{}'",
          execution.getPartition());
      repository.restartStage(executionId, stageId);
    }
    return execution;
  }

  private PipelineExecution updatePreconditionStageExpression(
      Map restartDetails, PipelineExecution execution) {
    List<Map> expressionRequestMap = getExpressionFromPreconditionStage(restartDetails);
    List<StageExecution> stagesToRemove = new ArrayList<>();
    for (StageExecution stage : execution.getStages()) {
      if (stage.getType() != null && !stage.getType().isEmpty()) {
        log.info("RJR -->  updatePreconditionStageExpression stage.getType {} ", stage.getType());
        log.info("RJR -->  updatePreconditionStageExpression with stage {} ", stage.getContext());
        if (stage.getType().equalsIgnoreCase("checkPreconditions")) {
          if (stage.getContext().get("preconditionType") != null
              && stage
                  .getContext()
                  .get("preconditionType")
                  .toString()
                  .equalsIgnoreCase("expression")) {
            log.info(
                "RJR -->  updatePreconditionStageExpression removing the stage {} ",
                stage.getContext());
            stagesToRemove.add(stage);
          } else {
            if (stage.getContext().get("preconditions") != null) {
              List<Map> preconditionsMap = (List<Map>) stage.getContext().get("preconditions");
              log.info(
                  "RJR -->  updatePreconditionStageExpression stage preconditionsMap {} ",
                  preconditionsMap);
              for (Map context : preconditionsMap) {
                if (context.get("type").toString().equalsIgnoreCase("expression")) {
                  Map expressionMap = (Map) context.get("context");
                  String expression = expressionMap.get("expression").toString();
                  log.info(
                      "RJR1 -->  updatePreconditionStageExpression final expression {} ",
                      expression);
                }
              }
              stage.getContext().replace("preconditions", expressionRequestMap);
              repository.storeStage(stage);
            }
          }
        }

        log.info(
            "RJR -->  updatePreconditionStageExpression with type {} stage context {} ",
            stage.getType(),
            stage.getContext());
      }
    }
    execution.getStages().removeAll(stagesToRemove);
    return execution;
  }

  private List<Map> getExpressionFromPreconditionStage(Map restartDetails) {
    List<Map> contextMap = new ArrayList();
    log.info(
        "RJR1 -->  getExpressionFromPreconditionStage with restartDetails {} ", restartDetails);
    Map pipelineConfigMap = new HashMap(restartDetails);
    log.info(
        "RJR1 -->  getExpressionFromPreconditionStage with pipelineConfigMap {} ",
        pipelineConfigMap);
    List<String> keysToRetain = new ArrayList<>();
    keysToRetain.add("stages");
    pipelineConfigMap.keySet().retainAll(keysToRetain);
    log.info(
        "RJR1 -->  getExpressionFromPreconditionStage pipelineConfigMap after retained with stages {} ",
        pipelineConfigMap);
    Map<String, List<Map>> stageMap = new HashMap(pipelineConfigMap);
    log.info("RJR1 -->  getExpressionFromPreconditionStage with stageMap {} ", stageMap);
    if (!stageMap.isEmpty() && stageMap != null) {
      List<Map> pipelineStageList = stageMap.get(keysToRetain.get(0));
      for (Map pipelineStage : pipelineStageList) {
        if (pipelineStage.get("type").toString().equalsIgnoreCase("checkPreconditions")) {
          contextMap = (List<Map>) pipelineStage.get("preconditions");
          for (Map context : contextMap) {
            if (context.get("type").toString().equalsIgnoreCase("expression")) {
              Map<String, Object> expressionMap = (Map<String, Object>) context.get("context");
              String expression = (String) expressionMap.get("expression");
              log.info(
                  "RJR1 -->  getExpressionFromPreconditionStage final expression {} ", expression);
            }
          }
        }
      }
    }
    return contextMap;
  }

  private PipelineExecution doInternal(
      Consumer<PipelineExecution> runnerAction,
      Runnable repositoryAction,
      String action,
      ExecutionType executionType,
      String executionId) {
    PipelineExecution toReturn = null;
    try {
      runWithRetries(repositoryAction);

      toReturn =
          runWithRetries(
              () -> {
                PipelineExecution execution = repository.retrieve(executionType, executionId);
                if (repository.handlesPartition(execution.getPartition())) {
                  runnerAction.accept(execution);
                } else {
                  log.info(
                      "Not pushing queue message action='{}' for execution with foreign partition='{}'",
                      action,
                      execution.getPartition());
                }
                return execution;
              });
    } catch (Exception e) {
      log.error(
          "Failed to {} execution with executionType={} and executionId={}",
          action,
          executionType,
          executionId,
          e);
    }
    return toReturn;
  }

  private <T> T runWithRetries(Supplier<T> action) {
    return retrySupport.retry(action, 5, Duration.ofMillis(100), false);
  }

  private void runWithRetries(Runnable action) {
    retrySupport.retry(
        () -> {
          action.run();
          return true;
        },
        5,
        Duration.ofMillis(100),
        false);
  }
}
