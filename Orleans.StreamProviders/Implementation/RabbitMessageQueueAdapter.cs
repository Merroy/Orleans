using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.StreamProviders.Implementation
{
    class RabbitMessageQueueAdapter : IQueueAdapter
    {
        protected readonly string DeploymentId;
        protected readonly string DataConnectionString;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        protected readonly ConcurrentDictionary<QueueId, RabbitMessageQueueDataManager> Queues = new ConcurrentDictionary<QueueId, RabbitMessageQueueDataManager>();

        public string Name { get; private set; }

        public bool IsRewindable
        {
            get { return false; }
        }

        public StreamProviderDirection Direction { get { return StreamProviderDirection.ReadWrite; } }

        public RabbitMessageQueueAdapter(HashRingBasedStreamQueueMapper streamQueueMapper, string dataConnectionString, string deploymentId, string providerName)
        {
            if (string.IsNullOrEmpty(dataConnectionString))
                throw new ArgumentNullException("dataConnectionString");
            if (string.IsNullOrEmpty(deploymentId))
                throw new ArgumentNullException("deploymentId");
            DataConnectionString = dataConnectionString;
            DeploymentId = deploymentId;
            Name = providerName;
            _streamQueueMapper = streamQueueMapper;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            RabbitMessageQueueDataManager queue;
            if (!Queues.TryGetValue(queueId, out queue))
            {
                var tmpQueue = new RabbitMessageQueueDataManager(queueId.ToString(), DeploymentId, DataConnectionString);
                await tmpQueue.InitQueueAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }
            var cloudMsg = RabbitMessageQueueBatchContainer.ToCloudQueueMessage(streamGuid, streamNamespace, events, requestContext);
            await queue.AddQueueMessage(cloudMsg);
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return RabbitMessageQueueAdapterReceiver.Create(queueId, DataConnectionString, DeploymentId);
        }
    }
}
