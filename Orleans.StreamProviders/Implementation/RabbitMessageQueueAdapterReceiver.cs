using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.StreamProviders.Implementation
{
    class RabbitMessageQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private RabbitMessageQueueDataManager _queue;
        private long _lastReadMessage;
        private Task _outstandingTask;
        private const int MaxNumberOfMessagesToPeek = 32;
        public QueueId Id { get; private set; }

        public static IQueueAdapterReceiver Create(QueueId queueId, string dataConnectionString, string deploymentId)
        {
            if (queueId == null)
                throw new ArgumentNullException("queueId");
            if (string.IsNullOrEmpty(dataConnectionString))
                throw new ArgumentNullException("dataConnectionString");
            if (string.IsNullOrEmpty(deploymentId))
                throw new ArgumentNullException("deploymentId");
            var queue = new RabbitMessageQueueDataManager(queueId.ToString(), deploymentId, dataConnectionString);
            return new RabbitMessageQueueAdapterReceiver(queueId, queue);
        }

        private RabbitMessageQueueAdapterReceiver(QueueId queueId, RabbitMessageQueueDataManager queue)
        {
            if (queueId == null)
                throw new ArgumentNullException("queueId");
            if (queue == null)
                throw new ArgumentNullException("queue");
            Id = queueId;
            _queue = queue;
        }

        public Task Initialize(TimeSpan timeout)
        {
            return _queue != null ? _queue.InitQueueAsync() : TaskDone.Done; // check in case we already shut it down.
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            try
            {
                if (_outstandingTask != null) // await the last storage operation, so after we shutdown and stop this receiver we don't get async operation completions from pending storage operations.
                    await _outstandingTask;
            }
            finally
            {
                _queue = null;  // remember that we shut down so we never try to read from the queue again.
            }
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
                if (queueRef == null) return new List<IBatchContainer>();
                var count = maxCount < 0 || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG ?
                    MaxNumberOfMessagesToPeek : Math.Min(maxCount, MaxNumberOfMessagesToPeek);
                var task = queueRef.GetQueueMessages(count);
                _outstandingTask = task;
                var messages = await task;
                var azureQueueMessages = messages
                    .Select(msg => (IBatchContainer)RabbitMessageQueueBatchContainer.FromCloudQueueMessage(msg, _lastReadMessage++)).ToList();
                return azureQueueMessages;
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.  
                if (messages.Count == 0 || queueRef == null)
                    return;
                var cloudQueueMessages = messages.Cast<RabbitMessageQueueBatchContainer>().Select(b => b.QueueMessage).ToList();
                _outstandingTask = Task.WhenAll(cloudQueueMessages.Select(queueRef.DeleteQueueMessage));
                await _outstandingTask;
            }
            finally
            {
                _outstandingTask = null;
            }
        }
    }
}
