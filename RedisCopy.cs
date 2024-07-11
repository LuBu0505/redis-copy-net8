
using Microsoft.VisualStudio.Services.Common;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace redis_copy
{
    internal class RedisCopy
    {
        #region parameters
        //The redis connection for source redis and its configuration options
        private RedisConnection sourcecon;
        private ConfigurationOptions sourceOptions;
        //The redis conneciton for destination redis and its configuration options
        private RedisConnection destcon;
        private ConfigurationOptions destOptions;
        //the flag to check all keys has been copy
        public bool allCopied = false;
        #endregion

        /// <summary>
        /// Init for Command args
        /// </summary>
        /// <param name="options"></param>
        public RedisCopy(Options options)
        {

            sourceOptions = new ConfigurationOptions();
            sourceOptions.EndPoints.Add(options.SourceEndpoint, options.SourcePort);
            sourceOptions.Ssl = options.SourceSSL;
            sourceOptions.Password = options.SourcePassword;
            sourceOptions.AllowAdmin = true;
            sourceOptions.ConnectRetry = 6;
            sourceOptions.ConnectTimeout = 10000;
            sourceOptions.SyncTimeout = 60000; // increasing timeout for source for SCAN command
            sourcecon = new RedisConnection(sourceOptions.ToString(), maxTaskCount);

            destOptions = new ConfigurationOptions();
            destOptions.EndPoints.Add(options.DestinationEndpoint, options.DestinationPort);
            destOptions.Ssl = options.DestinationSSL;
            destOptions.Password = options.DestinationPassword;
            destOptions.AllowAdmin = true;
            destOptions.ConnectRetry = 6;
            destOptions.ConnectTimeout = 10000;
            destcon = new RedisConnection(destOptions.ToString(), maxTaskCount);

        }

        /// <summary>
        /// Init for app setting 
        /// </summary>
        /// <param name="sourceConStr"></param>
        /// <param name="destConStr"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public RedisCopy(string? sourceConStr, string? destConStr)
        {
            if (string.IsNullOrEmpty(sourceConStr)) throw new ArgumentNullException(nameof(sourceConStr));
            if (string.IsNullOrEmpty(destConStr)) throw new ArgumentNullException(nameof(destConStr));

            Console.WriteLine($"== Source Redis ");
            sourceOptions = ConfigurationOptions.Parse(sourceConStr);
            sourceOptions.AllowAdmin = true;//Run info to get keyspace need enable the Admin
            sourceOptions.ConnectRetry = 6;
            sourceOptions.ConnectTimeout = 10000;
            sourceOptions.SyncTimeout = 60000;
            sourcecon = new RedisConnection(sourceOptions.ToString(), maxTaskCount);


            Console.WriteLine($"== Destination Redis ");
            destOptions = ConfigurationOptions.Parse(destConStr);
            destOptions.AllowAdmin = true;
            destOptions.AllowAdmin = true;
            destOptions.ConnectRetry = 6;
            destOptions.ConnectTimeout = 10000;
            destcon = new RedisConnection(destOptions.ToString(), maxTaskCount);
            Console.WriteLine("\n");
        }

        /// <summary>
        /// The entrance of copy redis from source to destination 
        /// </summary>
        public void Copy()
        {
            // Phase 1: Prepare the Source & Destation Info
            // Output the Source Redis Keyspace,  Memory used.
            PhaseOnePrepareCheckRedis();

            // Phase 2: Copy ...
            PhaseTwoRedisKeysMoveing().Wait();

            // Phase 3: Verify 
            PhaseThreeVerify().Wait();

            //END
            Console.WriteLine("Done copying.. All Keys moved, please input Y and then close it...");
            while (Console.ReadLine() != "Y")
            {
                Console.WriteLine("please input Y and then close it...");
            };
            Console.WriteLine("Done");
        }

        #region Phase 1:  Prepare the Source & Destation Info
        object lockObject = new object();
        long totalKeysCopied = 0;
        long totalKeysSource = 0;
        ConcurrentDictionary<long, long> dictdbIdxKeysNum = new ConcurrentDictionary<long, long>();

        int maxTaskCount = 20;
        private void PhaseOnePrepareCheckRedis()
        {
            Console.BackgroundColor = ConsoleColor.Blue;

            Console.WriteLine("Phase 1 - Check Source Keyspace & Memory ...");

            Console.ResetColor();

            Console.ForegroundColor = ConsoleColor.DarkGreen;

            Console.WriteLine("======================================================================");
            Console.WriteLine($"== Source:{sourceOptions.EndPoints[0].ToString()}");
            // Part 1: Output the keyspace & Memory 
            var infoGroup = sourcecon.BasicRetryInfo((conn) => conn.GetServer(conn.GetEndPoints()[0]).Info());

            foreach (var info in infoGroup)
            {
                if (info.Key.Equals("Memory"))
                {
                    Console.WriteLine($"==\t# {info.Key}");
                    var lists = info.ToList().Where(i => i.Key.Equals("used_memory_human") || i.Key.Equals("maxmemory_human")).ToList();
                    foreach (var list in lists)
                        Console.WriteLine($"==\t  {list.ToString()}");
                }

                if (info.Key.Equals("Keyspace"))
                {
                    Console.WriteLine($"==\t# {info.Key}");
                    foreach (var list in info.ToList())
                    {
                        long dbindex, dbkeys = 0;

                        long.TryParse(Regex.Match(list.Key, @"\d+\.*\d*").Value, out dbindex);
                        long.TryParse(list.Value.Split(new char[] { ',' })[0].Split(new char[] { '=' })[1], out dbkeys);

                        dictdbIdxKeysNum[dbindex] = dbkeys;

                        totalKeysSource += dbkeys;

                        Console.WriteLine($"==\t  {list.ToString()}");
                    }
                }
            }

            // Part 2: Split the keys patch to use SCAN command, like from 0 to 1000,  .... 3000 to 31000.  each batch get 1000 keys.


            foreach (var db in dictdbIdxKeysNum)
            {
                int batchIndex = 0;
                int startKeysIndex = 0;
                int batchSize = int.MaxValue;//less than 1000 to avoid the scan timeout exception
                do
                {
                    //Set the Skip Key Numbers.
                    startKeysIndex = batchIndex * batchSize;
                    //Adjust the Page size for the last one batch...
                    batchSize = (batchIndex + 1) * batchSize > db.Value ? (int)db.Value - batchIndex * batchSize : batchSize;

                    batchQueue.Enqueue(((int)db.Key, startKeysIndex, batchSize));

                    batchIndex++;

                } while (startKeysIndex + batchSize < db.Value);
            }

            Console.WriteLine("==");
            Console.WriteLine($"==\t{totalKeysSource} KEYS need to moving ... ...split to {batchQueue.Count} batch. ");

            Console.WriteLine("======================================================================");

            Console.ResetColor();
        }

        #endregion

        #region Phase 2: Copy ...
        private async Task PhaseTwoRedisKeysMoveing()
        {
            Console.BackgroundColor = ConsoleColor.Blue;
            Console.WriteLine("Phase 2 - Copy ...");
            Console.ResetColor();

            Console.ForegroundColor = ConsoleColor.DarkCyan;

            Console.WriteLine("======================================================================");

            #region comments the cluster 
            //cursorTop = Console.CursorTop;
            //if it's clustered cache loop on each of the shard and copy
            //var sourceClusternodes = GetClusterNodes(sourcecon);
            //if (sourceClusternodes != null)
            //{
            //    var config = ConfigurationOptions.Parse(sourcecon.Configuration);
            //    List<Tuple<string, long, double>> results = new List<Tuple<string, long, double>>();
            //    List<Task> copytasks = new List<Task>();
            //    foreach (var node in sourceClusternodes.Nodes)
            //    {
            //        if (!node.IsSlave)
            //        {
            //            //Console.WriteLine($"Copying keys from {sourceEndpointstring}");
            //            copytasks.Add(Task.Run(async () =>
            //            {
            //                var shardcon = GetConnectionMultiplexer(sourceEndpoint, (node.EndPoint as IPEndPoint).Port, config);
            //                var r = await Copy(shardcon);
            //                var sourceEndpointstring = $"{sourceEndpoint}:{(node.EndPoint as IPEndPoint).Port}";
            //                results.Add(new Tuple<string, long, double>(sourceEndpointstring, r.Item1, r.Item2));
            //                shardcon.Close();
            //            }));
            //            totalcursorsinProgress++;
            //        }
            //    }
            //    await Task.WhenAll(copytasks);
            //    var cursor = cursorTop + totalcursorsinProgress;
            //    foreach (var result in results)
            //    {
            //        Console.CursorTop = cursor++;
            //        Console.CursorLeft = 0;
            //        Console.WriteLine($"Copied {result.Item2} keys from {result.Item1} to {destEndpoint} in {result.Item3} seconds\n");
            //    }
            //    allCopied = true;
            //}
            //else
            //{
            //non clustered cache

            #endregion

            Console.WriteLine($"== Copying keys from {sourceOptions.EndPoints[0].ToString()}");

            var result = await StartMovingKeys();

            Console.WriteLine();
            Console.WriteLine($"****** Copied {result.Item1} keys from {sourceOptions.EndPoints[0].ToString()} to {destOptions.EndPoints[0].ToString()} in {result.Item2} seconds ******");
            allCopied = true;
            Console.WriteLine("======================================================================");
            Console.ResetColor();
        }



        List<(int, string)> verifiedKeys = new List<(int, string)>();
        //The Tuple Queue store the Redis Keys batch info.  format is (dbindex, startkeysindex, takekeysnumber)
        Queue<(int, int, int)> batchQueue = new Queue<(int, int, int)>();
        Queue<(int, int, int)> firstFailedQueue = new Queue<(int, int, int)>();

        private async Task<Tuple<long, double>> StartMovingKeys()
        {
            Stopwatch sw = Stopwatch.StartNew();

            ThreadPool.GetMinThreads(out maxTaskCount, out int p);
            using SemaphoreSlim semaphore = new SemaphoreSlim(maxTaskCount, maxTaskCount);
            Task[] tasks = new Task[batchQueue.Count];
            int index = 0;
            while (batchQueue.Count > 0)
            {
                if (semaphore.Wait(1000))
                {
                    tasks[index++] = Task.Factory.StartNew(() => subCopy(semaphore));
                }

                Thread.Sleep(1000);
                Console.WriteLine($"==  {batchQueue.Count} Taks leave in Queue....");
            }


            index = 0;
            //block to monitor completion
            while (totalKeysCopied < totalKeysSource)
            {
                await Task.Delay(2000);

                Console.Write($"== Coping progress ({totalKeysCopied}/{totalKeysSource} | {Math.Round(((double)totalKeysCopied / totalKeysSource) * 100)}% ). Below task still runing:\n==\t");
                Console.ForegroundColor = ConsoleColor.Green;
                int runningtask = 0;
                tasks.Where(t => t.Status != TaskStatus.RanToCompletion).ForEach(t => { Console.Write($"P_{t.Id} "); runningtask++; });
                Console.ForegroundColor = ConsoleColor.Blue;
                Console.Write($"({runningtask})\n");

                //If the coping phase have exception. will re-try copy those keys again and again.
                if (firstFailedQueue.Count > 0)
                {
                    Console.BackgroundColor = ConsoleColor.White;
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("==\tFirst Failed Queue list: " + firstFailedQueue.ToArray().Length);
                    Console.ResetColor();

                    if (semaphore.Wait(1000))
                    {
                        tasks[index++] = Task.Factory.StartNew(() => subCopy(semaphore));
                    }

                }
            }

            //// Wait for all tasks to complete.
            Task.WaitAll(tasks);
            //Task.WhenAll(tasks);

            Console.WriteLine("==\tAll tasks have completed.");

            sw.Stop();

            return new Tuple<long, double>(totalKeysCopied, Math.Round(sw.Elapsed.TotalSeconds));

        }

        public async void subCopy(SemaphoreSlim semaphore)
        {
            int dbindex = 0, skipKeys = 0, takeKeys = 0;

            try
            {
                lock (lockObject)
                {
                    if (batchQueue.Count > 0)
                    {
                        (dbindex, skipKeys, takeKeys) = batchQueue.Dequeue();
                    }
                    else if (firstFailedQueue.Count > 0)
                    {
                        Console.WriteLine($"== {firstFailedQueue.Count} Failed Task need re-run...");
                        (dbindex, skipKeys, takeKeys) = firstFailedQueue.Dequeue();
                    }
                    else
                    {
                        Console.WriteLine("==========================  Keys Move finished  ======================");
                        semaphore.Release();

                        return;
                    }
                }

                //Output the start info...
                Console.WriteLine($"==\tP_{Task.CurrentId},From:{skipKeys},To:{skipKeys + takeKeys},Task:{Task.CurrentId},Thread ID: {Thread.CurrentThread.ManagedThreadId}");

                Stopwatch swtask = Stopwatch.StartNew();
                ////Build the source & dest db object. Redis Connection 
                //var sourcedb = ConnectionMultiplexer.Connect(sourceOptions);
                //var destdb = ConnectionMultiplexer.Connect(destOptions);

                //var scansource = RedisConnection.InitializeAsync(sourceOptions.ToString()).Result;
                var allkeys = sourcecon.BasicRetryInfo((conn) => conn.GetServer(conn.GetEndPoints()[0]).Keys(dbindex).Skip(skipKeys).Take(takeKeys)).ToArray();

                Console.WriteLine($"==\tP_{Task.CurrentId},From:{skipKeys},To:{skipKeys + takeKeys},takeKeys:{takeKeys},Task:{Task.CurrentId},Thread ID: {Thread.CurrentThread.ManagedThreadId},start to moving... SCAN Time:{swtask.ElapsedMilliseconds} ms");

                swtask.Restart();
                var sourcedb = sourcecon.GetConection().GetDatabase(dbindex);
                var destdb = destcon.GetConection().GetDatabase(dbindex);


                foreach (var keys in SplitKeys(allkeys))
                {
                    var rbatch = sourcedb.CreateBatch();
                    var ttltask = new List<Task<TimeSpan?>>();
                    var dumptask = new List<Task<byte[]?>>();
                    foreach (var key in keys)
                    {
                        ttltask.Add(rbatch.KeyTimeToLiveAsync(key));

                        dumptask.Add(rbatch.KeyDumpAsync(key));
                    }
                    rbatch.Execute();

                    var ttlResults = Task.WhenAll(ttltask).Result;
                    var dumpkResults = Task.WhenAll(dumptask).Result;

                    //Restore the key to destation DB.
                    var destBatch = destdb.CreateBatch();

                    var i = 0;
                    foreach (var key in keys)
                    {
                        destBatch.KeyRestoreAsync(key, dumpkResults[i], ttlResults[i]);
                        i++;
                    }
                    destBatch.Execute();

                    //Random select one key to verify in Phase 3. 
                    if (keys.Count() > 0)
                    {
                        int index = RandomNumberGenerator.GetInt32(keys.Count());
                        verifiedKeys.Add((dbindex, keys.ElementAt<RedisKey>(index).ToString()));
                    }


                    lock (lockObject)
                    {
                        totalKeysCopied += keys.Count();
                    }
                }


                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.WriteLine($"==\tP_{Task.CurrentId},From:{skipKeys},To:{skipKeys + takeKeys},takeKeys:{takeKeys},Task:{Task.CurrentId},Thread ID: {Thread.CurrentThread.ManagedThreadId},  [DONE] , T/D/R Time:{swtask.ElapsedMilliseconds} ms");
                Console.ForegroundColor = ConsoleColor.DarkCyan;
                swtask.Stop();

            }
            catch (Exception ex)
            {
                lock (lockObject)
                {
                    firstFailedQueue.Enqueue((dbindex, skipKeys, takeKeys));
                }

                Console.BackgroundColor = ConsoleColor.DarkRed;
                Console.WriteLine($"=={DateTime.Now.ToLocalTime()}P_{Task.CurrentId}, failed ... " + ex.Message);
                Console.BackgroundColor = ConsoleColor.Black;
            }
            finally
            {
                semaphore.Release();
            }
        }


        /// <summary>
        /// Splits an array into several smaller arrays.
        /// </summary>
        /// <typeparam name="T">The type of the array.</typeparam>
        /// <param name="array">The array to split.</param>
        /// <param name="size">The size of the smaller arrays.</param>
        /// <returns>An array containing smaller arrays.</returns>
        private IEnumerable<IEnumerable<T>> SplitKeys<T>(T[] array, int size = 1000)
        {
            for (var i = 0; i < (float)array.Length / size; i++)
            {
                yield return array.Skip(i * size).Take(size);
            }
        }

        #endregion

        #region Phase 3: Verify 
        private async Task PhaseThreeVerify()
        {
            Console.BackgroundColor = ConsoleColor.Blue;
            Console.WriteLine("Phase 3: Verify  ...");
            Console.ResetColor();

            Thread.Sleep(1000);
            Console.ForegroundColor = ConsoleColor.DarkYellow;

            Console.WriteLine("======================================================================");

            Console.WriteLine($"== Random get ({verifiedKeys.Count}) keys to check the value between source and destination redis ...");
            Console.Write($"== ");
            foreach (var key in verifiedKeys)
            {
                try
                {
                    //getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync(key));

                    var sourdump = await sourcecon.BasicRetryInfo(async (sc) => sc.GetDatabase(key.Item1).KeyDumpAsync(key.Item2));
                    var destdump = await destcon.BasicRetryInfo(async (sc) => sc.GetDatabase(key.Item1).KeyDumpAsync(key.Item2));

                    if (!sourdump.Result.SequenceEqual(destdump.Result))
                    {
                        Console.Write($"\n");
                        Console.WriteLine($"== {key} Verify Failed");
                    }
                    else
                    {
                        Console.Write($"{key}, ");
                    }
                }
                catch (Exception ex)
                {
                    Console.BackgroundColor = ConsoleColor.Red;
                    Console.WriteLine($"=={DateTime.Now.ToLocalTime()} Verify {key} failed ({ex.Message})");
                    Console.BackgroundColor = ConsoleColor.Black;
                }
            }

            Console.Write($"\n");
            Console.WriteLine("======================================================================");
            Console.ResetColor();

        }

        #endregion

        #region cluster coding.. not used now @ 20240702
        //private ConnectionMultiplexer GetConnectionMultiplexer(string host, int port, ConfigurationOptions config)
        //{
        //    ConfigurationOptions tempconfig = new ConfigurationOptions();
        //    tempconfig.Ssl = config.Ssl;
        //    tempconfig.Password = config.Password;
        //    tempconfig.AllowAdmin = config.AllowAdmin;
        //    tempconfig.SyncTimeout = config.SyncTimeout;
        //    tempconfig.EndPoints.Add(host, port);
        //    return ConnectionMultiplexer.Connect(tempconfig);
        //}

        //private ClusterConfiguration GetClusterNodes(ConnectionMultiplexer conn)
        //{
        //    try
        //    {
        //        return conn.GetServer(conn.GetEndPoints()[0]).ClusterNodes();
        //    }
        //    catch
        //    {
        //        return null;
        //    }
        //}

        #endregion

    }
}