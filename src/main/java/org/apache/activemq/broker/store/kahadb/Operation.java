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
package org.apache.activemq.broker.store.kahadb;

import java.io.IOException;

import org.apache.activemq.broker.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.broker.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;

public abstract class Operation {
    
    protected final KahaDBStore store;
    final Location location;

    public Operation(KahaDBStore store, Location location) {
        this.store = store;
        this.location = location;
    }

    public Location getLocation() {
        return location;
    }

    abstract public void execute(Transaction tx) throws IOException;
    
    
    public static class AddOpperation extends Operation {
        final KahaAddMessageCommand command;

        public AddOpperation(KahaDBStore store, KahaAddMessageCommand command, Location location) {
            super(store, location);
            this.command = command;
        }

        public void execute(Transaction tx) throws IOException {
            store.upadateIndex(tx, command, location);
        }

        public KahaAddMessageCommand getCommand() {
            return command;
        }
    }
    
    public static class RemoveOpperation extends Operation {

        final KahaRemoveMessageCommand command;

        public RemoveOpperation(KahaDBStore store, KahaRemoveMessageCommand command, Location location) {
            super(store, location);
            this.command = command;
        }

        public void execute(Transaction tx) throws IOException {
            store.updateIndex(tx, command, location);
        }

        public KahaRemoveMessageCommand getCommand() {
            return command;
        }
    }
}