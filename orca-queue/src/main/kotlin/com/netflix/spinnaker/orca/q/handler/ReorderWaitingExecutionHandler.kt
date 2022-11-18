/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

package com.netflix.spinnaker.orca.q.handler

import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.CANCELED
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.NOT_STARTED
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.RUNNING
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.TERMINAL
import com.netflix.spinnaker.orca.api.pipeline.models.PipelineExecution
import com.netflix.spinnaker.orca.events.ExecutionComplete
import com.netflix.spinnaker.orca.events.ExecutionStarted
import com.netflix.spinnaker.orca.ext.initialStages
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import com.netflix.spinnaker.orca.q.CancelExecution
import com.netflix.spinnaker.orca.q.StartExecution
import com.netflix.spinnaker.orca.q.StartStage
import com.netflix.spinnaker.orca.q.ReorderWaitingExecution
import com.netflix.spinnaker.orca.q.pending.PendingExecutionService
import com.netflix.spinnaker.q.Queue
import java.time.Clock
import java.time.Instant
import net.logstash.logback.argument.StructuredArguments.value
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
class ReorderWaitingExecutionHandler(
  override val queue: Queue,
  override val repository: ExecutionRepository,
  private val pendingExecutionService: PendingExecutionService
) : OrcaMessageHandler<ReorderWaitingExecution> {

  override val messageType = ReorderWaitingExecution::class.java

  private val log: Logger get() = LoggerFactory.getLogger(javaClass)

  override fun handle(message: ReorderWaitingExecution) {
    log.info("RJR ReorderWaitingExecution handler called with reorderAction - {}, name - {}, id - {}", message.reorderAction, message.application, message.executionId)
    message.withExecution { execution ->
      log.info("RJR ReorderWaitingExecution handler called for execution {} {} {}", execution.application, execution.name, execution.id)
      log.info("RJR ReorderWaitingExecution for pipelineConfigId {} with reorderAction {}", execution.pipelineConfigId, execution.reorderAction)
      log.info("RJR withExecution with reorderAction - {}, name - {}, id - {}", message.reorderAction, message.application, message.executionId)
      if (execution.status == NOT_STARTED && !execution.isCanceled) {
        pendingExecutionService.reorderExecutions(execution.pipelineConfigId, execution.id, execution.reorderAction)
        log.info("RJR Done ReorderWaitingExecution pipelineConfigId {}", execution.pipelineConfigId)
      } else {
        pendingExecutionService.reorderExecutions(execution.pipelineConfigId, execution.id, execution.reorderAction)
        log.info("RJR The execution either already started or canceled, not reordering the execution {} ",execution.id)
      }
    }
  }
}
