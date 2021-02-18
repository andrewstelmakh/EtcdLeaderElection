using System;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Mvccpb;

namespace EtcdLeaderElection
{
    class Program
    {
        private const long Ttl = 5;
        private const string LeaderKey = "/leader";
        private static readonly EtcdClient EtcdClient = new EtcdClient("http://localhost:2379");
        
        static async Task Main(string[] args)
        {
            var serverName = args[0];
                
            while (true)
            {
                var leaderResult = await ElectLeader(serverName);
                if (leaderResult.Item1)
                {
                    Console.WriteLine("I am a leader.");
                    await LeadershipGained(leaderResult.Item2);
                    break;
                }
                
                Console.WriteLine("I am a follower.");
                await WaitForNextElection();
            }
        }

        private static async Task WaitForNextElection()
        {
            var watchId = 100500;
            var request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(LeaderKey),
                    WatchId = watchId
                }
            };

            var canObtainLeadership = false;
            
            EtcdClient.Watch(request, async response =>
            {
                foreach (var @event in response.Events)
                {
                    if (@event.Type == Event.Types.EventType.Delete)
                    {
                        Console.WriteLine("Leader died. Election needed.");
                        canObtainLeadership = true;
                    }
                }
            });

            while (!canObtainLeadership)
            {
                Console.WriteLine("Waiting for leader to die.");
                await DoWork();
            }
        }
        
        private static async Task LeadershipGained(long lease)
        {
            Console.WriteLine("Press ESC to terminate");

            while (!(Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Escape))
            {
                Console.WriteLine("Refreshing. Still a leader.");
                EtcdClient.LeaseKeepAlive(lease, CancellationToken.None);
                
                await DoWork();
            }

            await Revoke(lease);
        }

        private static async Task Revoke(long lease)
        {
            Console.WriteLine("Revoking leader lease. Terminating.");
            
            await EtcdClient.LeaseRevokeAsync(new LeaseRevokeRequest
            {
                ID = lease
            });
        }

        private static Task DoWork()
        {
            return Task.Delay(1000);
        }

        private static async Task<(bool, long)> ElectLeader(string serverName)
        {
            Console.WriteLine("Electing a leader.");
            
            var leaseResponse = await EtcdClient.LeaseGrantAsync(new LeaseGrantRequest
            {
                TTL = Ttl
            });

            var put = await TryPut(LeaderKey, serverName, leaseResponse.ID);
            return (put, leaseResponse.ID);
        }

        private static async Task<bool> TryPut(string key, string value, long lease)
        {
            var request = new TxnRequest();
            request.Success.Add(new RequestOp
            {
                RequestPut = new PutRequest
                {
                    Key = ByteString.CopyFromUtf8(key), 
                    Value = ByteString.CopyFromUtf8(value),
                    Lease = lease
                }
            });
            request.Compare.Add(new Compare {Version = 0, Key = ByteString.CopyFromUtf8(key)});
            request.Failure.Add(new RequestOp[0]);
         
            var response = await EtcdClient.TransactionAsync(request);
            return response.Succeeded;
        }
    }
}