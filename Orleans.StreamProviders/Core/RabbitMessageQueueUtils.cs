using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Orleans.StreamProviders.Core
{
    internal static class RabbitMessageQueueUtils
    {
        internal static IModel GetQueueChanel(string storageConnectionString)
        {
            if (storageConnectionString == null)
                throw new ArgumentNullException(nameof(storageConnectionString));
            var factory = new ConnectionFactory
            {
                Uri = storageConnectionString,
                AutomaticRecoveryEnabled = true,
                TaskScheduler = TaskScheduler.Current
            };
            var connection = factory.CreateConnection();
            return connection.CreateModel();
        }
    }
}