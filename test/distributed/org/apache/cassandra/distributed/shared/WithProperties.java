package org.apache.cassandra.distributed.shared;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.test.HostReplacementTest;

public final class WithProperties implements AutoCloseable
{
    private final List<State> properties = new ArrayList<>();

    public void with(String... kvs)
    {
        assert kvs.length % 2 == 0 : "Input must have an even amount of inputs but given " + kvs.length;
        for (int i = 0; i < kvs.length - 2; i = i + 2)
        {
            with(kvs[i], kvs[i + 1]);
        }
    }

    public void setProperty(String key, String value)
    {
        with(key, value);
    }

    public void with(String key, String value)
    {
        String previous = System.setProperty(key, value);
        properties.add(new State(key, previous));
    }


    @Override
    public void close()
    {
        properties.forEach(s -> {
            if (s.value == null)
                System.getProperties().remove(s.key);
            else
                System.setProperty(s.key, s.value);
        });
    }

    private static final class State
    {
        private final String key;
        private final String value;

        private State(String key, String value)
        {
            this.key = key;
            this.value = value;
        }
    }
}
