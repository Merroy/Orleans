using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.StreamProviders.Entity;
using Orleans.Streams;

namespace Orleans.StreamProviders.Implementation
{
    [Serializable]
    class RabbitMessageQueueBatchContainer : IBatchContainer
    {
        private EventSequenceToken _sequenceToken;
        private readonly List<object> _events;
        private readonly Dictionary<string, object> _requestContext;

        [NonSerialized]
        // Need to store reference to the original AQ CloudQueueMessage to be able to delete it later on.
        // Don't need to serialize it, since we are never interested in sending it to stream consumers.
        internal RabbitMessage QueueMessage;

        public Guid StreamGuid { get; private set; }
        public string StreamNamespace { get; private set; }

        public StreamSequenceToken SequenceToken
        {
            get { return _sequenceToken; }
        }

        private RabbitMessageQueueBatchContainer(Guid streamGuid, string streamNamespace, List<object> events, Dictionary<string, object> requestContext)
        {
            if (events == null)
                throw new ArgumentNullException("events", "Message contains no events");
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            _events = events;
            _requestContext = requestContext;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return _events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, _sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            // There is something in this batch that the consumer is intereted in, so we should send it.
            // Consumer is not interested in any of these events, so don't send.
            return _events.Any(item => shouldReceiveFunc(stream, filterData, item));
        }

        internal static RabbitMessage ToCloudQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var rabbitQueueBatchMessage = new RabbitMessageQueueBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = SerializationManager.SerializeToByteArray(rabbitQueueBatchMessage);
            return new RabbitMessage(rawBytes);
        }

        public bool ImportRequestContext()
        {
            if (_requestContext != null)
            {
                RequestContext.Import(_requestContext);
                return true;
            }
            return false;
        }

        internal static RabbitMessageQueueBatchContainer FromCloudQueueMessage(RabbitMessage cloudMsg, long sequenceId)
        {
            var rabbitQueueBatch = SerializationManager.DeserializeFromByteArray<RabbitMessageQueueBatchContainer>(cloudMsg.Body);
            rabbitQueueBatch.QueueMessage = cloudMsg;
            rabbitQueueBatch._sequenceToken = new EventSequenceToken(sequenceId);
            return rabbitQueueBatch;
        }

        public override string ToString()
        {
            return string.Format("[RabbitMessageQueueBatchContainer:Stream={0},#Items={1}]", StreamGuid, _events.Count);
        }
    }
}
