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

import java.net.InetAddress;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.DatabaseDescriptor;

public abstract class StreamRateLimiter
{
    protected static final double BYTES_PER_MEGABIT = (1024 * 1024) / 8; // from bits
    protected final boolean isLocalDC;

    public StreamRateLimiter(InetAddress peer)
    {
        if (DatabaseDescriptor.getLocalDataCenter() != null && DatabaseDescriptor.getEndpointSnitch() != null)
            isLocalDC = DatabaseDescriptor.getLocalDataCenter().equals(
            DatabaseDescriptor.getEndpointSnitch().getDatacenter(peer));
        else
            isLocalDC = true;
    }

    public abstract RateLimiter getLimiter();
    public abstract RateLimiter getInterDCLimiter();

    protected void setValidatedLimiterRate(RateLimiter rateLimiter, double limit)
    {
        // if throughput is set to 0, throttling is disabled
        if (limit == 0)
            limit = Double.MAX_VALUE;
        rateLimiter.setRate(limit);
    }

    public double acquire(int toTransfer)
    {
        double totalSleep = getLimiter().acquire(toTransfer);
        if (isLocalDC)
            return totalSleep;
        totalSleep += getInterDCLimiter().acquire(toTransfer);
        return totalSleep;
    }

    public double acquire(long toTransfer)
    {
        double totalSleep = 0.0;
        while (0 < toTransfer) {
            if (toTransfer < Integer.MAX_VALUE)
            {
                totalSleep += acquire((int)toTransfer);
                break;
            }
            totalSleep += acquire(Integer.MAX_VALUE);
            toTransfer -= Integer.MAX_VALUE;
        }
        return totalSleep;
    }

    /**
     * Gets streaming outbound rate limiter.
     * When stream_throughput_outbound_megabits_per_sec is 0, this returns rate limiter
     * with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @return StreamRateLimiter with rate limit set based on peer location.
     */
    public static StreamRateLimiter getOutboundRateLimiter(InetAddress peer)
    {
        return new StreamOutboundRateLimiter(peer);
    }

    private static class StreamOutboundRateLimiter extends StreamRateLimiter
    {
        private static final RateLimiter limiter = RateLimiter.create(Double.MAX_VALUE);
        private static final RateLimiter interDCLimiter = RateLimiter.create(Double.MAX_VALUE);

        public StreamOutboundRateLimiter(InetAddress peer)
        {
            super(peer);

            double throughput = DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() * BYTES_PER_MEGABIT;
            setValidatedLimiterRate(limiter, throughput);

            double interDCThroughput = DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec() * BYTES_PER_MEGABIT;
            setValidatedLimiterRate(interDCLimiter, interDCThroughput);
        }

        public RateLimiter getLimiter()
        {
            return limiter;
        }

        public RateLimiter getInterDCLimiter()
        {
            return interDCLimiter;
        }
    }

    /**
     * Gets streaming inbound rate limiter.
     * When stream_throughput_inbound_megabits_per_sec is 0, this returns rate limiter
     * with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @return StreamRateLimiter with rate limit set based on peer location.
     */
    public static StreamRateLimiter getInboundRateLimiter(InetAddress peer)
    {
        return new StreamInboundRateLimiter(peer);
    }

    private static class StreamInboundRateLimiter extends StreamRateLimiter
    {
        private static final RateLimiter limiter = RateLimiter.create(Double.MAX_VALUE);
        private static final RateLimiter interDCLimiter = RateLimiter.create(Double.MAX_VALUE);

        public StreamInboundRateLimiter(InetAddress peer)
        {
            super(peer);

            double throughput = DatabaseDescriptor.getStreamThroughputInboundMegabitsPerSec() * BYTES_PER_MEGABIT;
            setValidatedLimiterRate(limiter, throughput);

            double interDCThroughput = DatabaseDescriptor.getInterDCStreamThroughputInboundMegabitsPerSec() * BYTES_PER_MEGABIT;
            setValidatedLimiterRate(interDCLimiter, interDCThroughput);
        }

        public RateLimiter getLimiter()
        {
            return limiter;
        }

        public RateLimiter getInterDCLimiter()
        {
            return interDCLimiter;
        }
    }

}
