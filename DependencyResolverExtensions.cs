using System;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Messaging;

namespace Signalr.MongoDb
{
    public static class DependencyResolverExtensions
    {

        /// <summary>
        /// Use Mongo as the messaging backplane for scaling out of ASP.NET SignalR applications in a web farm.
        /// </summary>
        /// <param name="resolver">The dependency resolver</param>
        /// <param name="configuration">The Mongo scale-out configuration options.</param>
        /// <returns>The dependency resolver.</returns>
        public static IDependencyResolver UseMongoDb(this IDependencyResolver resolver, MongoScaleoutConfiguration configuration)
        {
            var _bus = new Lazy<MongoMessageBus>(() => new MongoMessageBus(resolver, configuration));
            resolver.Register(typeof(IMessageBus), () => _bus.Value);
            return resolver;
        }
    }
}
