﻿using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace redis_copy
{

    public class RedisConnection : IDisposable
    {
        private long _lastReconnectTicks = DateTimeOffset.MinValue.UtcTicks;
        private DateTimeOffset _firstErrorTime = DateTimeOffset.MinValue;
        private DateTimeOffset _previousErrorTime = DateTimeOffset.MinValue;

        // StackExchange.Redis will also be trying to reconnect internally,
        // so limit how often we recreate the ConnectionMultiplexer instance
        // in an attempt to reconnect
        private readonly TimeSpan ReconnectMinInterval = TimeSpan.FromSeconds(60);

        // If errors occur for longer than this threshold, StackExchange.Redis
        // may be failing to reconnect internally, so we'll recreate the
        // ConnectionMultiplexer instance
        private readonly TimeSpan ReconnectErrorThreshold = TimeSpan.FromSeconds(30);
        private readonly TimeSpan RestartConnectionTimeout = TimeSpan.FromSeconds(15);
        private const int RetryMaxAttempts = 5;

        private SemaphoreSlim _reconnectSemaphore = new SemaphoreSlim(initialCount: 1, maxCount: 1);
        private readonly string _connectionString;
        private ConnectionMultiplexer _connection;
        private readonly ConnectionMultiplexer[] _pool;
        private IDatabase _database;

        public RedisConnection(string connectionString, int poolSize)
        {
            _connectionString = connectionString;

            _pool = new ConnectionMultiplexer[poolSize];

            for (int i = 0; i < _pool.Length; i++)
            {
                if (_pool[i] == null)
                {
                    _pool[i] = ConnectionMultiplexer.Connect(_connectionString);
                }
                Console.WriteLine($"Conection {i + 1} /{poolSize} connected now ... ");
            }
        }

        public T BasicRetryInfo<T>(Func<ConnectionMultiplexer, T> func)
        {
            int reconnectRetry = 0;

            while (true)
            {
                if (_connection == null)
                {
                    _connection = GetConection();
                }

                try
                {
                    return func(_connection);
                }
                catch (Exception ex) when (ex is RedisConnectionException || ex is SocketException || ex is ObjectDisposedException)
                {
                    reconnectRetry++;
                    if (reconnectRetry > RetryMaxAttempts)
                    {
                        throw;
                    }

                    try
                    {
                        ForceReconnectAsync().Wait();
                    }
                    catch (ObjectDisposedException) { }
                }
            }
        }

        //public static async Task<RedisConnection> InitializeAsync(string connectionString)
        //{
        //    var redisConnection = new RedisConnection(connectionString);

        //    await redisConnection.ForceReconnectAsync(initializing: true);

        //    return redisConnection;
        //}

        // In real applications, consider using a framework such as
        // Polly to make it easier to customize the retry approach.
        // For more info, please see: https://github.com/App-vNext/Polly
        public async Task<T> BasicRetryAsync<T>(Func<IDatabase, Task<T>> func)
        {
            int reconnectRetry = 0;

            while (true)
            {
                if (_connection == null)
                {
                    _connection = GetConection();
                }

                try
                {
                    return await func(_database);
                }
                catch (Exception ex) when (ex is RedisConnectionException || ex is SocketException || ex is ObjectDisposedException)
                {
                    reconnectRetry++;
                    if (reconnectRetry > RetryMaxAttempts)
                    {
                        throw;
                    }

                    try
                    {
                        await ForceReconnectAsync();
                    }
                    catch (ObjectDisposedException) { }
                }
            }
        }

        public ConnectionMultiplexer GetConection()
        {
            var leastPendingTasks = long.MaxValue;
            ConnectionMultiplexer leastPendingDatabase = null;
            int useconn = 0;
            for (int i = 0; i < _pool.Length; i++)
            {
                var connection = _pool[i];

                if (connection == null)
                {
                    _pool[i] = ConnectionMultiplexer.Connect(_connectionString);

                    return _pool[i];
                }

                var pending = connection.OperationCount;

                if (pending < leastPendingTasks)
                {
                    leastPendingTasks = pending;
                    leastPendingDatabase = connection;
                    useconn = i;
                }
            }

            Console.WriteLine($"{Task.CurrentId} pick the redis connections {useconn}");

            return leastPendingDatabase;
        }


        /// <summary>
        /// Force a new ConnectionMultiplexer to be created.
        /// NOTES:
        ///     1. Users of the ConnectionMultiplexer MUST handle ObjectDisposedExceptions, which can now happen as a result of calling ForceReconnectAsync().
        ///     2. Call ForceReconnectAsync() for RedisConnectionExceptions and RedisSocketExceptions. You can also call it for RedisTimeoutExceptions,
        ///         but only if you're using generous ReconnectMinInterval and ReconnectErrorThreshold. Otherwise, establishing new connections can cause
        ///         a cascade failure on a server that's timing out because it's already overloaded.
        ///     3. The code will:
        ///         a. wait to reconnect for at least the "ReconnectErrorThreshold" time of repeated errors before actually reconnecting
        ///         b. not reconnect more frequently than configured in "ReconnectMinInterval"
        /// </summary>
        /// <param name="initializing">Should only be true when ForceReconnect is running at startup.</param>
        private async Task ForceReconnectAsync(bool initializing = false)
        {
            long previousTicks = Interlocked.Read(ref _lastReconnectTicks);
            var previousReconnectTime = new DateTimeOffset(previousTicks, TimeSpan.Zero);
            TimeSpan elapsedSinceLastReconnect = DateTimeOffset.UtcNow - previousReconnectTime;

            // We want to limit how often we perform this top-level reconnect, so we check how long it's been since our last attempt.
            if (elapsedSinceLastReconnect < ReconnectMinInterval)
            {
                return;
            }

            bool lockTaken = await _reconnectSemaphore.WaitAsync(RestartConnectionTimeout);
            if (!lockTaken)
            {
                // If we fail to enter the semaphore, then it is possible that another thread has already done so.
                // ForceReconnectAsync() can be retried while connectivity problems persist.
                return;
            }

            try
            {
                var utcNow = DateTimeOffset.UtcNow;
                previousTicks = Interlocked.Read(ref _lastReconnectTicks);
                previousReconnectTime = new DateTimeOffset(previousTicks, TimeSpan.Zero);
                elapsedSinceLastReconnect = utcNow - previousReconnectTime;

                if (_firstErrorTime == DateTimeOffset.MinValue && !initializing)
                {
                    // We haven't seen an error since last reconnect, so set initial values.
                    _firstErrorTime = utcNow;
                    _previousErrorTime = utcNow;
                    return;
                }

                if (elapsedSinceLastReconnect < ReconnectMinInterval)
                {
                    return; // Some other thread made it through the check and the lock, so nothing to do.
                }

                TimeSpan elapsedSinceFirstError = utcNow - _firstErrorTime;
                TimeSpan elapsedSinceMostRecentError = utcNow - _previousErrorTime;

                bool shouldReconnect =
                    elapsedSinceFirstError >= ReconnectErrorThreshold // Make sure we gave the multiplexer enough time to reconnect on its own if it could.
                    && elapsedSinceMostRecentError <= ReconnectErrorThreshold; // Make sure we aren't working on stale data (e.g. if there was a gap in errors, don't reconnect yet).

                // Update the previousErrorTime timestamp to be now (e.g. this reconnect request).
                _previousErrorTime = utcNow;

                if (!shouldReconnect && !initializing)
                {
                    return;
                }

                _firstErrorTime = DateTimeOffset.MinValue;
                _previousErrorTime = DateTimeOffset.MinValue;

                // Create a new connection
                ConnectionMultiplexer _newConnection = await ConnectionMultiplexer.ConnectAsync(_connectionString);

                // Swap current connection with the new connection
                ConnectionMultiplexer oldConnection = Interlocked.Exchange(ref _connection, _newConnection);

                Interlocked.Exchange(ref _lastReconnectTicks, utcNow.UtcTicks);
                IDatabase newDatabase = _connection.GetDatabase();
                Interlocked.Exchange(ref _database, newDatabase);

                if (oldConnection != null)
                {
                    try
                    {
                        await oldConnection.CloseAsync();
                    }
                    catch
                    {
                        // Ignore any errors from the old connection
                    }
                }
            }
            finally
            {
                _reconnectSemaphore.Release();
            }
        }

        public void Dispose()
        {
            try { _connection?.Dispose(); } catch { }
        }
    }

    public interface IConnectionMultiplexerPool
    {
        Task<IDatabase> GetDatabaseAsync();
    }

    public class ConnectionMultiplexerPool : IConnectionMultiplexerPool
    {
        private readonly ConnectionMultiplexer[] _pool;
        private readonly ConfigurationOptions _redisConfigurationOptions;

        public ConnectionMultiplexerPool(int poolSize, string connectionString) : this(poolSize, ConfigurationOptions.Parse(connectionString))
        {
        }

        public ConnectionMultiplexerPool(int poolSize, ConfigurationOptions redisConfigurationOptions)
        {
            _pool = new ConnectionMultiplexer[poolSize];
            _redisConfigurationOptions = redisConfigurationOptions;
        }

        public async Task<IDatabase> GetDatabaseAsync()
        {
            var leastPendingTasks = long.MaxValue;
            IDatabase leastPendingDatabase = null;

            for (int i = 0; i < _pool.Length; i++)
            {
                var connection = _pool[i];

                if (connection == null)
                {
                    _pool[i] = await ConnectionMultiplexer.ConnectAsync(_redisConfigurationOptions);

                    return _pool[i].GetDatabase();
                }

                var pending = connection.GetCounters().TotalOutstanding;

                if (pending < leastPendingTasks)
                {
                    leastPendingTasks = pending;
                    leastPendingDatabase = connection.GetDatabase();
                }
            }

            return leastPendingDatabase;
        }
    }
}
