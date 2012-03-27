/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.apollo.util


/*
  Simple trait to cut down on the code necessary to manage BaseService instances
 */

object ServiceControl {

  // start or stop a single service
  private def controlService(start: Boolean, service: Service, action: String) = {
    val tracker = new LoggingTracker(action)
    if (start) tracker.start(service) else tracker.stop(service)
    tracker.await
  }

  // start or stop a bunch of services in one go
  private def controlServices(start: Boolean, services: Seq[Service], action: String) = {
    val tracker = new LoggingTracker(action)
    services.foreach(service => {if (start) tracker.start(service) else tracker.stop(service)})
    tracker.await
  }

  def start(services: Seq[Service], action: String) = {
    controlServices(true, services, action)
  }

  def stop(services: Seq[Service], action: String) = {
    controlServices(false, services, action)
  }

  def start(service: Service, action: String) = {
    controlService(true, service, action)
  }

  def stop(service: Service, action: String) = {
    controlService(false, service, action)
  }

  def start(service: Service) = {
    controlService(true, service, "Starting "+service)
  }

  def stop(service: Service) = {
    controlService(false, service, "Stopping "+service)
  }

}