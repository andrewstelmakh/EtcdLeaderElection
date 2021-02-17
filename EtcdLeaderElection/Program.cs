using System;
using System.Threading.Tasks;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Google.Protobuf.Collections;

namespace EtcdLeaderElection
{
    class Program
    {
        private const long Ttl = 5;
        private const string LeaderKey = "Leader_Key";

        static async Task Main(string[] args)
        {
            var serverName = args[0];
            var etcdClient = new EtcdClient("http://localhost:2379");

            while (true)
            {
                var leaderResult = await ElectLeader(etcdClient, serverName);
                if (leaderResult.Item1)
                {
                    Console.WriteLine("I am the leader.");
                    await LeadershipGained(etcdClient, leaderResult.Item2);
                }
                else
                {
                    Console.WriteLine("I am a follower.");
                    await WitForNextElection(etcdClient);
                }
            }
        }

        private static async Task WitForNextElection(EtcdClient etcdClient)
        {
            var request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(LeaderKey)
                }
            };

            await etcdClient.Watch(request, print);
        }
        
        private static void print(WatchResponse response)
        {   
        }

        private static async Task LeadershipGained(EtcdClient etcdClient, long lease)
        {
            while (true)
            {
                try
                {
                    Console.WriteLine("Refreshing. Still a leader.");
                    //var response =  await etcdClient.LeaseTimeToLiveAsync(new LeaseTimeToLiveRequest());
                    await DoWork();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Revoking lease. No longer a leader.");
                    await etcdClient.LeaseRevokeAsync(new LeaseRevokeRequest() {ID = lease});
                }
            }
        }

        private static Task DoWork()
        {
            return Task.Delay(1000);
        }

        private static async Task<(bool, long)> ElectLeader(EtcdClient etcdClient, string serverName)
        {
            Console.WriteLine("Electing a leader.");
            
            var leaseResponse = await etcdClient.LeaseGrantAsync(new LeaseGrantRequest
            {
                TTL = Ttl
            });

            var put = await TryPut(etcdClient, LeaderKey, serverName, leaseResponse.ID);
            return (put, leaseResponse.ID);
        }

        private static async Task<bool> TryPut(EtcdClient etcdClient, string key, string value, long lease)
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
         
            var response = await etcdClient.TransactionAsync(request);
            return response.Succeeded;
        }
    }
}