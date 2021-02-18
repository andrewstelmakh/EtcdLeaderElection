using System;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using Mvccpb;

namespace EtcdLeaderElection
{
    class Program
    {
        private const long Ttl = 5;
        private const string LeaderKey = "/leader";
        private static readonly EtcdClient EtcdClient = new EtcdClient("http://localhost:2379");
        private static long Lease;
        
        static async Task Main(string[] args)
        {
            var serverName = args[0];

            while (true)
            {
                var leaderResult = await ElectLeader(serverName);
                if (leaderResult.Item1)
                {
                    Console.WriteLine("I am the leader.");
                    Console.CancelKeyPress += ConsoleOnCancelKeyPress;
                    await LeadershipGained(leaderResult.Item2);
                }
                else
                {
                    Console.WriteLine("I am a follower.");
                    await WitForNextElection();
                }
            }
        }

        private static void ConsoleOnCancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Console.WriteLine("Revoking leader lease");

            EtcdClient.LeaseRevokeAsync(new LeaseRevokeRequest()
            {
                ID = Lease
            });
            
            Environment.Exit(-1);
        }

        private static async Task WitForNextElection()
        {
            var request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(LeaderKey)
                },
            };

            Console.WriteLine("Waiting for leader to die.");
            
            var cancellationTokeSource = new CancellationTokenSource();
            
            try
            {
                await EtcdClient.Watch(request, async response =>
                {
                    foreach (var @event in response.Events)
                    {
                        if (@event.Type == Event.Types.EventType.Delete)
                        {
                            Console.WriteLine("Leader died. Cancelling watch.");
                            cancellationTokeSource.Cancel();
                        }
                    }
                }, cancellationToken: cancellationTokeSource.Token);
            }
            catch (RpcException)
            {
                Console.WriteLine("Watch cancelled");
            }
        }
        private static async Task LeadershipGained(long lease)
        {
            while (true)
            {
                try
                {
                    Console.WriteLine("Refreshing. Still a leader.");
                    await EtcdClient.LeaseKeepAlive(new LeaseKeepAliveRequest()
                    {
                        ID = lease
                    }, response =>
                    {

                    }, CancellationToken.None);
                    await DoWork();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Revoking lease. No longer a leader.");
                    await EtcdClient.LeaseRevokeAsync(new LeaseRevokeRequest() {ID = lease});
                }
            }
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

            Lease = leaseResponse.ID;

            var put = await TryPut(LeaderKey, serverName, Lease);
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