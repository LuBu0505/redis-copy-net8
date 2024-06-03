using redis_copy;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.Configuration;

var options = Options.Parse(args);
if (options != null)
{
    Console.WriteLine("Get the input parameter by args ... ");
    Console.WriteLine("++++++++++++++++++++++++++++++++++++++++++++++++++++ ");
    var copy = new RedisCopy(options);
    copy.Copy();
}
else
{

    Console.WriteLine("++++++++++++++++++++++++++++++++++++++++++++++++++++ ");
    Console.WriteLine("No Args, Check the AppSettings.json content ... "); 
    // Get the Redis Connection String from AppSettings.json file
    var config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("AppSettings.json", optional: false).Build();

    string? sourceConStr = config["SourceRedisConnectionString"];
    string? destConStr = config["DestRedisConnectionString"];
    string? destFlushData = config["DestinationFlush"];
    string? dbindex = config["DBToCopy"];

    var copy = new RedisCopy(sourceConStr, destConStr, destFlushData, dbindex);

    copy.Copy();
}
