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
package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Encapsulates the behavior for 'locking' any streamed sttables to a node.
 * If a process crashes while converting a set of SSTableWriters to SSTReaders
 * (meaning, some subset of SSTWs were converted, but not the entire set), we want
 * to disregard the entire set as we will surely have missing data (by definition).
 *
 * Basic behavior id to write out the names of all SSTWs to a file, one SSTW per line,
 * and then delete the file when complete (normal behavior). This should happen before
 * converting any SSTWs. Thus, the lockfile is created, some SSTWs are converted,
 * and if the process crashes, on restart, we look for any existing lockfile, and delete
 * any referenced SSTRs.
 */
public class StreamLockfile
{
    public static final String FILE_EXT = ".lockfile";
    private static final Logger logger = LoggerFactory.getLogger(StreamLockfile.class);

    private final File lockfile;
    private final HashSet<String> sstablesToSkipOnCleanup = new HashSet<>();

    public StreamLockfile(File directory, UUID cfId)
    {
        this.lockfile = new File(directory, cfId.toString() + FILE_EXT);
    }

    public StreamLockfile(File lockfile)
    {
        assert lockfile != null;
        this.lockfile = lockfile;
    }

    public void append(SSTableWriter sstable)
    {
        /* write out the file names *without* the 'tmp-file' flag in the file name.
           this class will not need to clean up tmp files (on restart), CassandraDaemon does that already,
           just make sure we delete the fully-formed SSTRs. */
        String sstablePath = sstable.descriptor.asType(Descriptor.Type.FINAL).baseFilename();

        try
        {
            Files.write(lockfile.toPath(), Collections.singleton(sstablePath), Charsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                        StandardOpenOption.DSYNC);
        }
        catch (IOException e)
        {
            logger.warn(String.format("Could not create lockfile %s for stream session, nothing to worry too much about", lockfile), e);
        }
    }

    public void delete()
    {
        if (exists())
        {
            FileUtils.delete(lockfile);
            sstablesToSkipOnCleanup.clear();
        }
    }

    protected boolean exists()
    {
        return lockfile.exists();
    }

    public void cleanup()
    {
        List<String> files = readLockfile(lockfile);
        for (String file : files)
        {
            try
            {
                Descriptor desc = Descriptor.fromFilename(file, true);
                String sstableName = desc.filenameFor(Component.DATA);
                if (!sstablesToSkipOnCleanup.contains(sstableName))
                {
                    SSTable.delete(desc, SSTable.componentsFor(desc));
                }
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn("failed to delete a potentially stale sstable {}", file);
            }
        }
    }

    private List<String> readLockfile(File lockfile)
    {
        try
        {
            return Files.readAllLines(lockfile.toPath(), Charsets.UTF_8);
        }
        catch (IOException e)
        {
            logger.info("couldn't read lockfile {}, ignoring", lockfile.getAbsolutePath());
            return Collections.emptyList();
        }
    }

    public void skipOnCleanup(SSTableReader reader)
    {
        this.sstablesToSkipOnCleanup.add(reader.getFilename());
    }
}
