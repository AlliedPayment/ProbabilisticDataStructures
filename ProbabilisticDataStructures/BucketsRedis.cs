using System;
using System.Collections;
using System.ComponentModel;
using System.Diagnostics;
using BloomFilter.Redis;
using StackExchange.Redis;

namespace ProbabilisticDataStructures
{
    /// <summary>
    /// Buckets is a fast, space-efficient array of buckets where each bucket can store
    /// up to a configured maximum value.
    /// </summary>
    public class BucketsRedis
    {
        private byte bucketSize { get; set; }
        private RedisBitOperate _redis;
        public string _redisKey;
        internal uint count { get; set; }
        private byte _max;
        private int Max
        {
            get
            {
                return _max;
            }
            set
            {
                // TODO: Figure out this truncation thing.
                // I'm not sure if MaxValue is always supposed to be capped at 255 via
                // a byte conversion or not...
                if (value > byte.MaxValue)
                    _max = byte.MaxValue;
                else
                    _max = (byte)value;
            }
        }

        /// <summary>
        /// Creates a new Buckets with the provided number of buckets where each bucket
        /// is the specified number of bits.
        /// </summary>
        /// <param name="count">Number of buckets.</param>
        /// <param name="bucketSize">Number of bits per bucket.</param>
        /// <param name="connectString">Redis connection string</param>
        internal BucketsRedis(uint count, byte bucketSize, string connectString= "localhost", string redisKey = "")
        {
            var config=ConfigurationOptions.Parse(connectString);
            this.count = count;
            this.bucketSize = bucketSize;
            this.Max = (1 << bucketSize) - 1;
            this._redis = new RedisBitOperate(config);
            this._redisKey = redisKey;
        }

        /// <summary>
        /// Returns the maximum value that can be stored in a bucket.
        /// </summary>
        /// <returns>The bucket max value.</returns>
        internal byte MaxBucketValue()
        {
        	return this._max;
        }

        /// <summary>
        /// Increment the value in the specified bucket by the provided delta. A bucket
        /// can be decremented by providing a negative delta.
        /// <para>
        ///     The value is clamped to zero and the maximum bucket value. Returns itself
        ///     to allow for chaining.
        /// </para>
        /// </summary>
        /// <param name="bucket">The bucket to increment.</param>
        /// <param name="delta">The amount to increment the bucket by.</param>
        /// <returns>The modified bucket.</returns>
        internal BucketsRedis Increment(uint bucket, int delta)
        {
            var t= (uint)bucket * this.bucketSize;
            this._redis.IncrBitField(this._redisKey, this.bucketSize,t , delta);
            return this;
        }

        /// <summary>
        /// Set the bucket value. The value is clamped to zero and the maximum bucket
        /// value. Returns itself to allow for chaining.
        /// </summary>
        /// <param name="bucket">The bucket to change the value of.</param>
        /// <param name="value">The value to set.</param>
        /// <returns>The modified bucket.</returns>
        internal BucketsRedis Set(uint bucket, byte value)
        {
            if (value > this._max)
                value = this._max;
            SetBits(bucket, bucketSize, value);
            return this;
        }

        /// <summary>
        /// Returns the value in the specified bucket.
        /// </summary>
        /// <param name="bucket">The bucket to get.</param>
        /// <returns>The specified bucket.</returns>
        internal int Get(uint bucket)
        {
            return (int)this._redis.GetBitField(this._redisKey, this.bucketSize, bucket);
        }

        /// <summary>
        /// Restores the Buckets to the original state. Returns itself to allow for
        /// chaining.
        /// </summary>
        /// <returns>The Buckets object the reset operation was performed on.</returns>
        internal BucketsRedis Reset()
        {
            this._redis.Clear(this._redisKey);
            return this;
        }


        /// <summary>
        /// Returns the bits at the specified offset and length.
        /// </summary>
        /// <param name="offset">The position to start reading at.</param>
        /// <param name="length">The distance to read from the offset.</param>
        /// <returns>The bits at the specified offset and length.</returns>
        internal RedisResult GetBits(uint offset, uint length)
        {
            return this._redis.GetBitField(this._redisKey, length, offset);
        }

        /// <summary>
        /// Sets bits at the specified offset and length.
        /// </summary>
        /// <param name="offset">The position to start writing at.</param>
        /// <param name="length">The distance to write from the offset.</param>
        /// <param name="bits">The bits to write.</param>
        internal RedisResult SetBits(uint offset, uint length, byte bits)
        {
            return this._redis.SetBitField(this._redisKey, this.bucketSize, offset,bits);
        }
    }
}
