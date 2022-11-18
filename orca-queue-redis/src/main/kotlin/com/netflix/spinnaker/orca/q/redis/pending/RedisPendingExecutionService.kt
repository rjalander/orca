/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.spinnaker.orca.q.redis.pending

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.orca.q.pending.PendingExecutionService
import com.netflix.spinnaker.q.Message
import redis.clients.jedis.Jedis
import redis.clients.jedis.util.Pool
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Map
import java.util.HashMap
import kotlin.collections.List
import kotlin.collections.MutableMap

class RedisPendingExecutionService(
  private val pool: Pool<Jedis>,
  private val mapper: ObjectMapper
) : PendingExecutionService {

  private val log: Logger get() = LoggerFactory.getLogger(javaClass)

  override fun enqueue(pipelineConfigId: String, message: Message) {
    pool.resource.use { redis ->
      redis.lpush(listName(pipelineConfigId), mapper.writeValueAsString(message))
    }
  }

  override fun popOldest(pipelineConfigId: String): Message? =
    pool.resource.use { redis ->
      redis
        .rpop(listName(pipelineConfigId))
        ?.let { mapper.readValue(it) }
    }

  override fun reorderExecutions(pipelineConfigId: String) {
    pool.resource.use { redis ->
      if (redis.llen(listName(pipelineConfigId)) > 2) {
        log.info("RJR running reorderExecutions list size is {}", redis.llen(listName(pipelineConfigId)))
        var firstEle = redis.lindex(listName(pipelineConfigId), 0)
        var secondEle = redis.lindex(listName(pipelineConfigId), 1)
        var thirdEle = redis.lindex(listName(pipelineConfigId), 2)
//        var lastEle = redis.lindex(listName(pipelineConfigId), redis.llen(listName(pipelineConfigId))-1)

        log.info("RJR running reorderExecutions firstEle {}", firstEle)
        log.info("RJR running reorderExecutions secondEle {}", secondEle)
        log.info("RJR running reorderExecutions thirdEle {}", thirdEle)

        redis.lset(listName(pipelineConfigId), 0, thirdEle)
        redis.lset(listName(pipelineConfigId), 2, firstEle)

        var firstEle1 = redis.lindex(listName(pipelineConfigId), 0)
        var secondEle1 = redis.lindex(listName(pipelineConfigId), 1)
        var thirdEle1 = redis.lindex(listName(pipelineConfigId), 2)

        log.info("RJR running reorderExecutions firstEle1 {}", firstEle1)
        log.info("RJR running reorderExecutions secondEle1 {}", secondEle1)
        log.info("RJR running reorderExecutions thirdEle1 {}", thirdEle1)
        //redis.rpush(listName(pipelineConfigId), firstEle)

        //redis.lpop(listName(pipelineConfigId))
      }else{
        log.info("RJR NOT running reorderExecutions list size is {}", redis.llen(listName(pipelineConfigId)))

      }
    }
  }

  override fun reorderExecutions(pipelineConfigId: String, id: String, reorderAction: String) {
    log.info("RJR running Redis reorderExecutions with pipelineConfigId {}, id {}, reorderAction {}", pipelineConfigId, id, reorderAction)
    pool.resource.use { redis ->
      val listSize = redis.llen(listName(pipelineConfigId))
      log.info("RJR running reorderExecutions list size is {}", listSize)
      if (listSize > 2) {
        var currentExecution = ""
        run breaking@ {
          redis.lrange(listName(pipelineConfigId), 0, listSize-1).forEach { execution ->
            var executionMap: Map<String,Object> = mapper.readValue(execution)
            val executionId = executionMap.get("executionId") as String
            if (executionId.equals(id)){
              currentExecution = execution;
              return@breaking
            }
          }
        }
        log.info("RJR reorderExecutions currentExecution is {}", currentExecution)
        val indexScript : String = "local key = KEYS[1]\n" +
          "local obj = ARGV[1]\n" +
          "local items = redis.call('lrange', key, 0, -1)\n" +
          "for i=1,#items do\n" +
          "    if items[i] == obj then\n" +
          "        return i - 1\n" +
          "    end\n" +
          "end \n" +
          "return -1"
        var currentIndex = redis.eval(indexScript, 1, listName(pipelineConfigId), currentExecution) as Long
        log.info("RJR reorderExecutions currentIndex is {}", currentIndex)
        var otherIndex : Long = -1
        if (reorderAction.equals("UP", true) && (currentIndex < listSize-1)) {
          otherIndex = (currentIndex + 1)
        } else if (reorderAction.equals("DOWN", true) && (currentIndex > 0) && (currentIndex < listSize)){
          otherIndex = (currentIndex - 1)
        }else{
          log.info("RJR Re-order is not supported with the reorderAction {} and currentIndex {}", reorderAction, currentIndex)
        }
        log.info("RJR reorderExecutions otherIndex is {}", otherIndex)
        if (otherIndex != -1L){
          var otherExecution = redis.lindex(listName(pipelineConfigId), otherIndex)
          log.info("RJR reorderExecutions otherExecution is {}", otherExecution)

          var firstEle = redis.lindex(listName(pipelineConfigId), 0)
          var secondEle = redis.lindex(listName(pipelineConfigId), 1)
          var thirdEle = redis.lindex(listName(pipelineConfigId), 2)

          log.info("RJR before running reorderExecutions firstEle {}", firstEle)
          log.info("RJR before running reorderExecutions secondEle {}", secondEle)
          log.info("RJR before running reorderExecutions thirdEle {}", thirdEle)

          redis.lset(listName(pipelineConfigId), currentIndex, otherExecution)
          redis.lset(listName(pipelineConfigId), otherIndex, currentExecution)

          var firstEle1 = redis.lindex(listName(pipelineConfigId), 0)
          var secondEle1 = redis.lindex(listName(pipelineConfigId), 1)
          var thirdEle1 = redis.lindex(listName(pipelineConfigId), 2)

          log.info("RJR after running reorderExecutions firstEle1 {}", firstEle1)
          log.info("RJR after running reorderExecutions secondEle1 {}", secondEle1)
          log.info("RJR after running reorderExecutions thirdEle1 {}", thirdEle1)
        }else{
          log.info("RJR Failed to get the otherIndex for re-ordering the execution with executionID {} ",id)
        }
      }else{
        log.info("RJR NOT running reorderExecutions list size is {}", redis.llen(listName(pipelineConfigId)))
      }
    }
  }


  override fun popNewest(pipelineConfigId: String): Message? =
    pool.resource.use { redis ->
      redis
        .lpop(listName(pipelineConfigId))
        ?.let { mapper.readValue(it) }
    }

  override fun purge(pipelineConfigId: String, callback: (Message) -> Unit) {
    pool.resource.use { redis ->
      while (redis.llen(listName(pipelineConfigId)) > 0L) {
        popOldest(pipelineConfigId)?.let(callback)
      }
    }
  }

  override fun depth(pipelineConfigId: String): Int =
    pool.resource.use { redis ->
      redis.llen(listName(pipelineConfigId)).toInt()
    }

  override fun pendingIds(): List<String> =
    throw NotImplementedError("only implemented on SqlPendingExecutionService")

  private fun listName(pipelineConfigId: String) =
    "orca.pipeline.queue.$pipelineConfigId"
}
