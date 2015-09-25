using System;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.StreamProviders.Implementation
{
    public class RabbitMessageQueueAdapterFactory : IQueueAdapterFactory
    {
        private const string CACHE_SIZE_PARAM = "CacheSize";
        private const int DEFAULT_CACHE_SIZE = 4096;
        private const string NUM_QUEUES_PARAM = "NumQueues";

        /// <summary> Default number oi\f Azure Queue used in this stream provider.</summary>
        public const int DEFAULT_NUM_QUEUES = 10; // keep as power of 2.

        private string _deploymentId;
        private string _dataConnectionString;
        private string _providerName;
        private int _cacheSize;
        private int _numQueues;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;

        /// <summary>"DataConnectionString".</summary>
        public const string DATA_CONNECTION_STRING = "DataConnectionString";
        /// <summary>"DeploymentId".</summary>
        public const string DEPLOYMENT_ID = "DeploymentId";

        /// <summary> Init the factory.</summary>
        public virtual void Init(IProviderConfiguration config, string providerName, Logger logger)
        {
            if (config == null) throw new ArgumentNullException("config");
            if (!config.Properties.TryGetValue(DATA_CONNECTION_STRING, out _dataConnectionString))
                throw new ArgumentException(string.Format("{0} property not set", DATA_CONNECTION_STRING));
            if (!config.Properties.TryGetValue(DEPLOYMENT_ID, out _deploymentId))
                throw new ArgumentException(string.Format("{0} property not set", DEPLOYMENT_ID));
            string cacheSizeString;
            _cacheSize = DEFAULT_CACHE_SIZE;
            if (config.Properties.TryGetValue(CACHE_SIZE_PARAM, out cacheSizeString))
                if (!int.TryParse(cacheSizeString, out _cacheSize))
                    throw new ArgumentException(string.Format("{0} invalid.  Must be int", CACHE_SIZE_PARAM));
            string numQueuesString;
            _numQueues = DEFAULT_NUM_QUEUES;
            if (config.Properties.TryGetValue(NUM_QUEUES_PARAM, out numQueuesString))
                if (!int.TryParse(numQueuesString, out _numQueues))
                    throw new ArgumentException(string.Format("{0} invalid.  Must be int", NUM_QUEUES_PARAM));
            _providerName = providerName;
            _streamQueueMapper = new HashRingBasedStreamQueueMapper(_numQueues, providerName);
            _adapterCache = new SimpleQueueAdapterCache(this, _cacheSize, logger);
        }

        /// <summary>Creates the RabbitMQ Queue based adapter.</summary>
        public virtual Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new RabbitMessageQueueAdapter(_streamQueueMapper, _dataConnectionString, _deploymentId, _providerName);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        /// <summary>Creates the adapter cache.</summary>
        public virtual IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        /// <summary>Creates the factory stream queue mapper.</summary>
        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _streamQueueMapper;
        }

        /// <summary>
        /// Creates a delivery failure handler for the specified queue.
        /// </summary>
        /// <param name="queueId"></param>
        /// <returns></returns>
        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));
        }
    }
}
