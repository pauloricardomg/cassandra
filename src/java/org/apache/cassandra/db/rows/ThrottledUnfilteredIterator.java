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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * A utility class to split the given {@link#UnfilteredRowIterator} into smaller chunks each 
 * having at most {@link #throttle} + 1 unfiltereds. Only the first output contains partition 
 * level info: {@link UnfilteredRowIterator#partitionLevelDeletion} and {@link UnfilteredRowIterator#staticRow}
 * 
 * The lifecycle of outputed {{@link UnfilteredRowIterator} only last till next call to {@link #next()},
 * It must be exhausted before calling another {@link #next()}
 * 
 * The given iterator should be closed by caller.
 * 
 */
public class ThrottledUnfilteredIterator extends AbstractIterator<UnfilteredRowIterator>
{
    private final UnfilteredRowIterator origin;
    private final int throttle;

    // internal mutable state
    private UnfilteredRowIterator throttledItr;
    // current batch's openMarker. if it's generated in previous batch,
    // it must be consumed as first element of current batch
    private RangeTombstoneMarker openMarker;
    // extra unfiltereds used to close current batch's openMarker: closeMarker
    private Iterator<Unfiltered> extended = Collections.emptyIterator();

    public ThrottledUnfilteredIterator(UnfilteredRowIterator origin, int throttle)
    {
        assert origin != null;
        assert throttle >= 1;
        this.origin = origin;
        this.throttle = throttle;
        this.throttledItr = null;
    }

    @Override
    protected UnfilteredRowIterator computeNext()
    {
        // previous throttled-output must be exhausted
        assert throttledItr == null || !throttledItr.hasNext();

        if (!origin.hasNext())
            return endOfData();

        throttledItr = new WrappingUnfilteredRowIterator(origin)
        {
            private int count = 0;
            private boolean isFirst = throttledItr == null;

            @Override
            public boolean hasNext()
            {
                return (withinLimit() && wrapped.hasNext()) || extended.hasNext();
            }

            @Override
            public Unfiltered next()
            {
                if (extended.hasNext())
                {
                    assert !withinLimit();
                    return extended.next();
                }

                Unfiltered next;
                assert withinLimit();
                // in the beginning of the batch, need to include previous remaining openMarker
                if (count == 0 && openMarker != null)
                    next = openMarker;
                else
                    next = wrapped.next();
                recordNext(next);
                return next;
            }

            private void recordNext(Unfiltered unfiltered)
            {
                count++;
                if (unfiltered.isRangeTombstoneMarker())
                    updateMarker((RangeTombstoneMarker) unfiltered);
                // when reach throttle with a remaining openMarker, we need to create corresponding closeMarker.
                if (count == throttle && openMarker != null)
                {
                    assert wrapped.hasNext();
                    extended = closeOpenMarker(wrapped.next()).iterator();
                }
            }

            private boolean withinLimit()
            {
                return count < throttle;
            }

            private void updateMarker(RangeTombstoneMarker marker)
            {
                openMarker = marker.isOpen(isReverseOrder()) ? marker : null;
            }

            /**
             * There 3 cases for next, 1. if it's boundaryMarker, we split it as closeMarker for current batch, next
             * openMarker for next batch 2. if it's boundMakrer, it must be closeMarker. 3. if it's Row. we create
             * corresponding closeMarker for current batch including the Row, create next openMarker for next batch.
             */
            private Collection<Unfiltered> closeOpenMarker(Unfiltered next)
            {
                assert openMarker != null;

                if (next.isRangeTombstoneMarker())
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) next;
                    // if it's boundary, create closeMarker for current batch and openMarker for next batch
                    if (marker.isBoundary())
                    {
                        RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                        updateMarker(boundary.createCorrespondingOpenMarker(isReverseOrder()));
                        return Collections.singleton(boundary.createCorrespondingCloseMarker(isReverseOrder()));
                    }
                    else
                    {
                        // if it's bound, it must be closeMarker.
                        assert marker.isClose(isReverseOrder());
                        updateMarker(marker);
                        return Collections.singleton(marker);
                    }
                }
                else
                {
                    // it's Row, need to create closeMarker for current batch and openMarker for next batch
                    DeletionTime openDeletion = openMarker.openDeletionTime(isReverseOrder());
                    ByteBuffer[] buffers = next.clustering().getRawValues();
                    RangeTombstoneBoundMarker closeMarker = RangeTombstoneBoundMarker.exclusiveClose(isReverseOrder(),
                                                                                                     buffers,
                                                                                                     openDeletion);
                    updateMarker(closeMarker);

                    // for next batch
                    updateMarker(RangeTombstoneBoundMarker.inclusiveOpen(isReverseOrder(),
                                                                         buffers,
                                                                         openDeletion));
                    return Arrays.asList(closeMarker, next);
                }
            }

            @Override
            public DeletionTime partitionLevelDeletion()
            {
                return isFirst ? wrapped.partitionLevelDeletion() : DeletionTime.LIVE;
            }

            @Override
            public Row staticRow()
            {
                return isFirst ? wrapped.staticRow() : Rows.EMPTY_STATIC_ROW;
            }

            @Override
            public void close()
            {
                // no op
            }
        };
        return throttledItr;
    }
}
