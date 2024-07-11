**Author:** Bu Lu ([@bulu](https://github.com/LuBu0505))  
**Reference:** A simple tool to copy Redis data from one redis endpoint to another, Convert to .NET 8 based on the orignal repository: https://github.com/deepakverma/redis-copy



# Redis Copy .NET8
The Redis Copy Console Tool allows users to copy Redis data from one redis endpoint to another.  

![Redis Copy .NET8](/images/image-0.png)
*redis cluster is not support.


# Software requirements
The following software is required to run Redis Copy tool. It may run on other versions.

- .NET 8
- VS Code / Visual Studio 2022


### Download the Source Code

```
clone https://github.com/LuBu0505/redis-copy-net8.git
```


## Using


- Option 1 -- Use AppSetting.json 

>  Replace the "< ... >" by real redis endpoint

```
{
  "SourceRedisConnectionString": "<source redis name>:6380,password=<your password>,ssl=True,abortConnect=False", //Source Redis ConnectionString
  "DestRedisConnectionString": "<Destination redis name>:6380,password=<your password>,ssl=True,abortConnect=False" //Destination Redis ConnectionString
}
```
![app setting](/images/image-1.png)

- Option 2 -- Use command parameter 

```
redis-copy-net8.exe
Parameter Description:
  --se           Required. SourceEndpoint *.redis.cache.windows.net
  --sa           Required. Source password
  --sp           (Default: 6380) Source port
  --sssl         (Default: true) Connect Source over ssl

  --de           Required. DestinationEndpoint *.redis.cache.windows.net
  --da           Required. Destination Password
  --dp           (Default: 6380) Destination port
  --dssl         (Default: true) Destination Source over ssl
  --help         Display this help screen.
  --version      Display version information.

eg:

redis-copy-net8.exe --se <xxxxxx.redis.cache.chinacloudapi.cn> --sa <******************> --de <xxxxxx.redis.cache.chinacloudapi.cn> --da <******************> 

```
![set arguments](/images/image-2.png)


## Redis Copy Process flow

Phase 1: Prepare the Source & Destation Info
- Check the source redis Used Memory, Keyspace 
- According to the Keys number to split to more sub task

Phase 2: Copy ...
- Loop the Keys sub task , use SCAN to list all keys at first.
- Create more task to use StackExchange.Redis bacth operation for TTL , DUMP
- Use batch opeartion to Restore the Keys to Destination Redis 
- If met Exception, Add the Keys info to Failed Queue. 
- Check the moved keys grogress, also check the failed queue, if not empty, will rerun the move task

Phase 3: Verify 
- Random pick some key to check the DUMP value one by one


## Documentation

TODO: More details are coming soon...