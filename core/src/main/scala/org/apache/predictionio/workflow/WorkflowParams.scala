/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.predictionio.workflow

case class AppParams(
  appId: String = "",
  channelId: String = "",
  rootPath: String = ""
)

/** Workflow parameters.
  *
  * @param batch Batch label of the run.
  * @param verbose Verbosity level.
  * @param saveModel Controls whether trained models are persisted.
  * @param sparkEnv Spark properties that will be set in SparkConf.setAll().
  * @param skipSanityCheck Skips all data sanity check.
  * @param stopAfterRead Stops workflow after reading from data source.
  * @param stopAfterPrepare Stops workflow after data preparation.
  * @group Workflow
  */
case class WorkflowParams(
  batch: String = "",
  verbose: Int = 2,
  saveModel: Boolean = true,
  appParams: AppParams = AppParams(),
  sparkEnv: Map[String, String] =
    Map[String, String]("spark.executor.extraClassPath" -> "."),
  skipSanityCheck: Boolean = false,
  stopAfterRead: Boolean = false,
  stopAfterPrepare: Boolean = false) {
  // Temporary workaround for WorkflowParamsBuilder for Java. It doesn't support
  // custom spark environment yet.
  def this(batch: String, verbose: Int, saveModel: Boolean,appParams: AppParams)
  = this(batch, verbose, saveModel, appParams, Map[String, String]())
}
