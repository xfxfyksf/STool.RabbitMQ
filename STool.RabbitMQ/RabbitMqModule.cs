using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Transactions;

namespace STool.RabbitMQ
{
    public class RabbitMqModule : IDisposable
    {
        public IConnectionPool ConnectionPool { get; }
        public IChannelPool ChannelPool { get; }
        public RabbitMqOptions Options { get; }

        public RabbitMqMessageConsumerFactory RabbitMqMessageConsumerFactory { get; }


        private ConcurrentDictionary<string, MessageResponse> _cache { get; set; } = new ConcurrentDictionary<string, MessageResponse>();

        public RabbitMqModule(RabbitMqOptions options)
        {
         
            Options = options;
            foreach (var connectionFactory in options.Connections.Values)
            {
                connectionFactory.DispatchConsumersAsync = true;

            }
            ConnectionPool = new ConnectionPool(options);
            ChannelPool = new ChannelPool(ConnectionPool);
            RabbitMqMessageConsumerFactory = new RabbitMqMessageConsumerFactory(ConnectionPool);
        }


        public IRabbitMqMessageConsumer CreateConsumer(ExchangeDeclareConfiguration exchangeDeclare, QueueDeclareConfiguration queueDeclare, string? connectionName = "Default")
        {
            return RabbitMqMessageConsumerFactory.Create(exchangeDeclare, queueDeclare, connectionName);
        }

        public RabbitMqBasicConfig GetProducterConfig(string serverName = "Default", string producterName = "Default")
        {
            return Options.Settings.GetOrDefault(serverName).Producters.GetOrDefault(producterName);
        }

        public AsyncEventingBasicConsumer? Publish(string exchangeName,string queueName, string routingKey, string message, bool isRpc, IBasicProperties? basicProperties = null, string serverName = "Default", string producterName = "Default", string connectionName = "Default")
        {
            var data = Encoding.UTF8.GetBytes(message);
            return Publish(exchangeName, queueName, routingKey, data, isRpc, basicProperties: basicProperties, serverName: serverName, producterName: producterName, connectionName: connectionName);
        }

        public AsyncEventingBasicConsumer? Publish(string exchangeName,string queueName, string routingKey, byte[] message, bool isRpc, bool mandatory = false, IBasicProperties? basicProperties = null, string serverName = "Default", string producterName = "Default", string connectionName = "Default")
        {
            AsyncEventingBasicConsumer? consumer = null;
            string replyTo = string.Empty;
            string correlationId = string.Empty;
            if (basicProperties != null)
            {
                replyTo = basicProperties.ReplyTo;
                correlationId = basicProperties.CorrelationId;
            }

            try
            {
                using (var channelAccessor = ChannelPool.Acquire(connectionName))
                {
                    var config = GetProducterConfig(serverName, producterName);

                    if (string.IsNullOrWhiteSpace(replyTo))
                    {
                        if (isRpc)
                        {
                            // 发送 RPC 消息并监听回复
                            // declare a server-named queue
                            string replyQueueName = channelAccessor.Channel.QueueDeclare().QueueName;
                            consumer = new AsyncEventingBasicConsumer(channelAccessor.Channel);
                            string consumerTag = channelAccessor.Channel.BasicConsume(consumer: consumer,
                                                                 queue: replyQueueName,
                                                                 autoAck: true);

                            basicProperties = channelAccessor.Channel.CreateBasicProperties();
                            basicProperties.ContentType = "text/plain";
                            basicProperties.DeliveryMode = 0x01;
                            basicProperties.Priority = 0x00;
                            basicProperties.ReplyTo = replyQueueName;
                            basicProperties.CorrelationId = Guid.NewGuid().ToString();

                            // 超时取消消费
                            Task.Run(() =>
                            {
                                Task.Delay(TimeSpan.FromSeconds(100)).Wait();
                                if (!string.IsNullOrWhiteSpace(consumerTag))
                                {
                                    channelAccessor.Channel.BasicCancelNoWait(consumerTag);
                                }
                            });
                        }
                        else
                        {
                            basicProperties = null;
                        }
                        
                        channelAccessor.Channel.ExchangeDeclare(exchange: exchangeName, type: config.Exchange.Type, config.Exchange.Durable, config.Exchange.AutoDelete, config.Exchange.Arguments);
                        channelAccessor.Channel.QueueDeclare(queueName, config.Queue.Durable, config.Queue.Exclusive, config.Queue.AutoDelete, config.Queue.Arguments);
                        channelAccessor.Channel.QueueBind(queueName, exchange: exchangeName, routingKey: routingKey, config.Queue.Arguments);
                    }
                    else
                    {
                        // 需要回复 RPC 消息
                        exchangeName = "";
                        routingKey = replyTo;
                        basicProperties = channelAccessor.Channel.CreateBasicProperties();
                        basicProperties.ContentType = "text/plain";
                        basicProperties.DeliveryMode = 0x01;
                        basicProperties.Priority = 0x00;
                        basicProperties.CorrelationId = correlationId;
                    }

                    channelAccessor.Channel.BasicPublish(exchange: exchangeName,
                                             routingKey: routingKey,
                                             mandatory: mandatory,
                                             basicProperties: basicProperties,
                                             body: message);
                    return consumer;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
           
        }

        public Task<MessageResponse> PublishAndWait(string exchangeName,string queueName, string routingKey, string message, string transActionId, string serverName = "Default", string producterName = "Default", string connectionName = "Default")
        {
            try
            {
                Task<MessageResponse> task = new Task<MessageResponse>(() =>
                {
                    Publish(exchangeName: exchangeName, queueName: queueName, routingKey: routingKey, message: message, isRpc: false, serverName: serverName, producterName: producterName, connectionName: connectionName);
                    MessageResponse result = new MessageResponse();
                    _cache.TryAdd(transActionId, result);
                    int waitTime = 20;
                    while (waitTime > 0)
                    {
                        _cache.TryGetValue(transActionId, out result);

                        if (result != null && result.Message != null)
                        {
                            return result;
                        }
                        Thread.Sleep(500);
                        waitTime--;
                    }

                    _cache.TryRemove(transActionId, out result);
                    result = new MessageResponse();
                    result.ReturnCode = 99;
                    result.ReturnMessage = "TimeOut";
                    return result;
                });

                task.Start();

                return task;
            }
            catch (Exception ex)
            {
                MessageResponse result = new MessageResponse();
                result.ReturnCode = 99;
                result.ReturnMessage = $"Exception:{ex.Message}";
                return Task.FromResult<MessageResponse>(result);
            }
        }

        public MessageResponse? FindRequestData(string transActionId)
        {
            _cache.TryGetValue(transActionId, out var result);
            return result;
        }

        public void SetResponseData(string transActionId, MessageResponse response)
        {
            _cache.TryAdd(transActionId, response);
        }

        public void Dispose()
        {
            ConnectionPool?.Dispose();
            ChannelPool?.Dispose();
        }
       
    }
}
