using Orleans.Providers.Streams.Common;
using Orleans.StreamProviders.Implementation;

namespace Orleans.StreamProviders
{
    public class RabbitMessageQueueStreamProvider : PersistentStreamProvider<RabbitMessageQueueAdapterFactory>
    {
    }
}
