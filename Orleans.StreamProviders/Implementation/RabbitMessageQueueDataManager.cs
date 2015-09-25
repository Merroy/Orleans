using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.StreamProviders.Core;
using Orleans.StreamProviders.Entity;
using RabbitMQ.Client;

namespace Orleans.StreamProviders.Implementation
{
    class RabbitMessageQueueDataManager
    {
        public string QueueName { get; private set; }

        private readonly IModel _queueClient;
        private readonly IBasicProperties _basicProperties;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queueName">Name of the queue to be connected to.</param>
        /// <param name="storageConnectionString">Connection string for the Azure storage account used to host this table.</param>
        public RabbitMessageQueueDataManager(string queueName, string storageConnectionString)
        {
            QueueName = queueName;
            _queueClient = RabbitMessageQueueUtils.GetQueueChanel(storageConnectionString);
            _basicProperties = GetBasicParameters();
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queueName">Name of the queue to be connected to.</param>
        /// <param name="deploymentId">The deployment id of the Azure service hosting this silo. It will be concatenated to the queueName.</param>
        /// <param name="storageConnectionString">Connection string for the Azure storage account used to host this table.</param>
        public RabbitMessageQueueDataManager(string queueName, string deploymentId, string storageConnectionString)
        {
            QueueName = string.Concat(deploymentId, "-", queueName);
            _queueClient = RabbitMessageQueueUtils.GetQueueChanel(storageConnectionString);
            _basicProperties = GetBasicParameters();
        }

        private IBasicProperties GetBasicParameters()
        {
            var property = _queueClient.CreateBasicProperties();
            property.Persistent = true;
            return property;
        }

        /// <summary>
        /// Initializes the connection to the queue.
        /// </summary>
        public Task InitQueueAsync()
        {
            var startTime = DateTime.UtcNow;
            try
            {
                _queueClient.QueueDeclare(QueueName, true, false, false, null);
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "CreateIfNotExist");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "InitQueue_Async");
            }
            return TaskDone.Done;
        }

        /// <summary>
        /// Deletes the queue.
        /// </summary>
        public Task DeleteQueue()
        {
            _queueClient.QueueDelete(QueueName);
            return TaskDone.Done;
        }

        /// <summary>
        /// Clears the queue.
        /// </summary>
        public Task ClearQueue()
        {
            _queueClient.QueuePurge(QueueName);
            return TaskDone.Done;
        }

        /// <summary>
        /// Adds a new message to the queue.
        /// </summary>
        /// <param name="message">Message to be added to the queue.</param>
        public async Task AddQueueMessage(RabbitMessage message)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                await Task.Factory.StartNew(() =>
                {
                    _queueClient.BasicPublish(string.Empty, QueueName, _basicProperties, message.Body);
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }
        }

        /// <summary>
        /// Gets a number of new messages from the queue.
        /// </summary>
        /// <param name="count">Number of messages to get from the queue.</param>
        public async Task<IEnumerable<RabbitMessage>> GetQueueMessages(int count = -1)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                return await Task.Factory.StartNew(() =>
                {
                    var results = new List<RabbitMessage>();
                    var i = count;
                    while (i > 0 || i == -1)
                    {
                        var result = _queueClient.BasicGet(QueueName, false);
                        if (result == null)
                            break;
                        results.Add(new RabbitMessage(result.Body, result.DeliveryTag));
                        if (i != -1)
                            i--;
                    }
                    return results.AsEnumerable();
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage");
                return null;
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }
        }

        /// <summary>
        /// Deletes a messages from the queue.
        /// </summary>
        /// <param name="message">A message to be deleted from the queue.</param>
        public async Task DeleteQueueMessage(RabbitMessage message)
        {
            var startTime = DateTime.UtcNow;
            try
            {
                await Task.Factory.StartNew(() =>
                {
                    _queueClient.BasicAck(message.DeliveryTag, false);
                });
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "DeleteMessage");
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "DeleteQueueMessage");
            }
        }


        private void CheckAlertSlowAccess(DateTime startOperation, string operation)
        {

        }

        private void ReportErrorAndRethrow(Exception exc, string operation)
        {

        }
    }
}
