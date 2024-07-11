using redis_copy;
using Microsoft.Extensions.Configuration;

ThreadPool.SetMinThreads(100, 100);


Console.WriteLine("======================================================================");
Console.WriteLine("==                    Redis Copy Tool by .NET 8                     ==");
Console.WriteLine($"==                    {DateTime.Now.ToLocalTime()}                           ==");
Console.WriteLine("======================================================================");

if (args.Count() > 0)
{

    var options = Options.Parse(args);
    //Console.WriteLine("Get the input parameter by args ... "); 
    var copy = new RedisCopy(options);
    copy.Copy();
}
else
{
    //Console.WriteLine("No Args, Check the AppSettings.json content  ");
    // Get the Redis Connection String from AppSettings.json file
    var config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("AppSettings.json", optional: false).Build();

    string? sourceConStr = config["SourceRedisConnectionString"];
    string? destConStr = config["DestRedisConnectionString"];

    var copy = new RedisCopy(sourceConStr, destConStr);

    copy.Copy();
}
