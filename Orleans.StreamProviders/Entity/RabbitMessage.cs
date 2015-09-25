using System;

namespace Orleans.StreamProviders.Entity
{
    [Serializable]
    public class RabbitMessage
    {
        public RabbitMessage(byte[] body, ulong deliveryTag)
        {
            Body = body;
            DeliveryTag = deliveryTag;
        }

        public RabbitMessage(byte[] body)
        {
            Body = body;
        }

        /// <summary>
        /// Retrieves the body of this message.
        /// 
        /// </summary>
        public byte[] Body { get; private set; }

        /// <summary>
        /// Retrieve the delivery tag for this message. See also <see cref="M:RabbitMQ.Client.IModel.BasicAck(System.UInt64,System.Boolean)"/>.
        /// </summary>
        public ulong DeliveryTag { get; private set; }
    }
}