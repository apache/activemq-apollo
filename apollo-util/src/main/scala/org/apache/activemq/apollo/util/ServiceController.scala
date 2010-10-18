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

import collection.mutable.ListBuffer

/*
  Simple trait to cut down on the code necessary to manage BaseService instances
 */

trait ServiceController {

  // start or stop a single service
  def controlService(start: Boolean, service: Service, action: String) = {
    val tracker = new LoggingTracker(action)
    if (start) tracker.start(service) else tracker.stop(service)
    tracker.await
  }

  // start or stop a bunch of services in one go
  def controlServices(start: Boolean, services: ListBuffer[Service], action: String) = {
    val tracker = new LoggingTracker(action)
    services.foreach((service: Service) => {if (start) tracker.start(service) else tracker.stop(service)})
    tracker.await
  }


}