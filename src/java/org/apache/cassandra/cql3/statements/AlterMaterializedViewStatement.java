/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.db.view.MaterializedView;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class AlterMaterializedViewStatement extends SchemaAlteringStatement
{
    private final TableAttributes attrs;

    public AlterMaterializedViewStatement(CFName name, TableAttributes attrs)
    {
        super(name);
        this.attrs = attrs;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        CFMetaData baseCf = MaterializedView.findBaseCf(keyspace(), columnFamily());
        if (baseCf == null)
            throw new InvalidRequestException("View '" + cfName + "' could not be found in any of the tables of keyspace '" + keyspace() + '\'');
        state.hasColumnFamilyAccess(keyspace(), baseCf.cfName, Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        if (!meta.isMaterializedView())
            throw new InvalidRequestException("Cannot use ALTER MATERIALIZED VIEW on Table");

        CFMetaData cfm = meta.copy();

        if (attrs == null)
            throw new InvalidRequestException("ALTER MATERIALIZED VIEW WITH invoked, but no parameters found");

        attrs.validate();
        cfm.params(attrs.asAlteredTableParams(cfm.params));

        MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);
        return true;
    }

    public String toString()
    {
        return String.format("AlterMaterializedViewStatement(name=%s)", cfName);
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
