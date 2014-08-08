using System;
using Microsoft.AspNet.SignalR.Messaging;
using System.Configuration;

namespace Signalr.MongoDb
{
    public class MongoScaleoutConfiguration : ScaleoutConfiguration
    {

        public TimeSpan RetryDelay { get; set; }
        public long CollectionMaxSize { get; set; }
        public long MaxDocuments { get; set; }
        public string ConnectionString { get; set; }
        public MongoScaleoutConfiguration(string connectionName)
        {
            ConnectionString = ConfigurationManager.ConnectionStrings[connectionName].ConnectionString;

            CollectionMaxSize = 2147483648;//2 Gb

            RetryDelay = TimeSpan.FromSeconds(2);

            MaxDocuments = 10000;
        }
    }
}
