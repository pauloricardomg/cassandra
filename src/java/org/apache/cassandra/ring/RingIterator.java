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

package org.apache.cassandra.ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.ring.token.VirtualNode;

public class RingIterator implements Iterator<VirtualNode>
{
    private final ArrayList<VirtualNode> sortedTokens;
    private int initalIndex = 0;
    private int currentIndex = 0;
    private boolean consumed = false;
    private boolean hasNext;

    public RingIterator(ArrayList<VirtualNode> sortedTokens)
    {
        this.sortedTokens = sortedTokens;
        this.hasNext = sortedTokens.size() > 0;
    }

    public boolean hasNext()
    {
        return hasNext;
    }

    public void advanceToToken(Token token)
    {
        currentIndex = firstTokenIndex(sortedTokens, VirtualNode.initial(token, null, null, null));
        if (!consumed)
            initalIndex = currentIndex;
    }

    public VirtualNode next()
    {
        if (!hasNext)
            throw new NoSuchElementException();

        if (!consumed)
            consumed = true;

        int next = currentIndex;
        currentIndex = (currentIndex + 1) % sortedTokens.size();
        hasNext = currentIndex != initalIndex;

        return sortedTokens.get(next);
    }

    public VirtualNode peekNextFromRack(String rack)
    {
        return null;
    }

    /**
     * Copy from {@link org.apache.cassandra.locator.TokenMetadata#firstTokenIndex(ArrayList, Token, boolean)}
     */
    private static int firstTokenIndex(final ArrayList<VirtualNode> ring, VirtualNode start)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = 0;
        }
        return i;
    }
}
