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

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.ring.token.TokenState;

public class RingIterator
{
    private final ArrayList<TokenState> sortedTokens;
    private int currentIndex = 0;
    private boolean consumed = false;

    public RingIterator(ArrayList<TokenState> sortedTokens)
    {
        this.sortedTokens = sortedTokens;
    }

    public boolean hasWrappedAround()
    {
        return consumed && currentIndex == 0;
    }

    public void advanceToToken(Token token)
    {
        currentIndex = firstTokenIndex(sortedTokens, TokenState.initial(token, null, null, null));
    }

    public TokenState next()
    {
        int next = currentIndex;
        currentIndex = (currentIndex + 1) % sortedTokens.size();
        consumed = true;
        return sortedTokens.get(next);
    }

    public TokenState peekNextFromRack(String rack)
    {
        return null;
    }

    /**
     * Copy from {@link org.apache.cassandra.locator.TokenMetadata#firstTokenIndex(ArrayList, Token, boolean)}
     */
    private static int firstTokenIndex(final ArrayList<TokenState> ring, TokenState start)
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
