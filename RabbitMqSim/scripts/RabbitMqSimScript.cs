﻿using RabbitMqSim;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using STool.Protocol;
using STool.RabbitMQ;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Serialization;

public class RabbitMqSimScript : IMessageHandler
{

    RabbitMqModule rabbitMqModule;

    public event LogDelegate LogHandler;
    public event LogDelegate ReceivedLogHandler;
    public IRabbitMqMessageConsumer mesConsumer;
    public void Start()
    {
        try
        {
            LogHandler?.Invoke("Load Config");
            var configStr = File.ReadAllText("config.json");
            var jsonDoc = JsonDocument.Parse(configStr);
            var options = jsonDoc.RootElement.GetProperty("RabbitMQ").Deserialize<RabbitMqOptions>();

            //若使用ASP.NET 可以使用IOC注入option进此模块
            rabbitMqModule = new RabbitMqModule(options);
            //获取默认消费配置
            var defaultConsumerConfig = rabbitMqModule.Options.Settings.GetOrDefault().Consumers.GetOrDefault();
            //创建消费者，创建exchange和queue
            mesConsumer = rabbitMqModule.CreateConsumer(defaultConsumerConfig.Exchange, defaultConsumerConfig.Queue);
            //设定绑定规则
            mesConsumer.BindAsync(defaultConsumerConfig.RoutingKey);
            //设定消息消费处
            mesConsumer.OnMessageReceived((model, args) =>
            {
                var body = args.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                LogHandler?.Invoke($"Received : {message}");
                ReceivedLogHandler?.Invoke(message);
                Handle(message, args.BasicProperties);
                return Task.CompletedTask;
            });
        }
        catch (Exception ex)
        {
            LogHandler?.Invoke(ex.Message + ex.StackTrace + ex.InnerException?.Message + ex.InnerException?.StackTrace);
        }

    }

    public void Handle(string message, IBasicProperties basicProperties)
    {
        try
        {
            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.LoadXml(message);
            var msgHeader = xmlDocument.GetHeader();
            switch (msgHeader.MESSAGENAME)
            {
                case "EapMesLotInfoRequest":
                    {
                        var requestBody = xmlDocument.GetBody<LotRequestTemplete>();
                        if (requestBody == null)
                        {
                            LogHandler?.Invoke($"EapMesLotInfoRequest body get fail");
                        }
                        MessageFormat<LotReplyTempleteBody> replyMsg =
                          new("MesEapLotInfoRequestReply", new()
                          {
                              MACHINENAME = requestBody.MACHINENAME,
                              PORTNAME = requestBody.PORTNAME,
                              PORTTYPE = requestBody.PORTTYPE,
                              CARRIERNAME = requestBody.CARRIERNAME,
                              OPERATORID = requestBody.OPERATORID,
                              BATCHID = "",
                              SLOTMAP = "3",
                              PRODUCTLIST = new List<LotReplyTempleteBody.Product>()
                              {
                                  new LotReplyTempleteBody.Product()
                                  {
                                      PRODUCTNAME="",
                                      LOTNO="",
                                      RECIPENAME="",
                                      RECIPEPARAMLIST=new List<LotReplyTempleteBody.Product.RecipeParam>(){},
                                      MASKTITLE="",
                                      PRODUCTTYPE="",
                                      PRODUCTIONTYPE="",
                                      PROCESSOPERATIONNAME="",
                                      WORKORDER="",
                                      SLOTNO="",
                                      INPUTACTION="",
                                      BUFFERSLOTNO="",
                                      ROTATION="",
                                      INSPECTIONNAME=""
                                  }
                              }
                          }) ;

                        Publish(replyMsg, basicProperties, msgHeader.TRANSACTIONID, true);
                    }
                    break;
                case "EapMesTrackInRequest":
                    {
                        //解析Body的对象需标注xmlroot为Body
                        var requestBody = xmlDocument.GetBody<LotReplyTempleteBody>();
                        MessageFormat<LotReplyTempleteBody> replyMsg =
                          new("MesEapTrackInRequestReply", new()
                          {
                              MACHINENAME = requestBody.MACHINENAME,
                              PORTNAME = requestBody.PORTNAME,
                              PORTTYPE = requestBody.PORTTYPE,
                              CARRIERNAME = requestBody.CARRIERNAME,
                              OPERATORID = requestBody.OPERATORID,
                              BATCHID = "",
                              SLOTMAP = "3",
                              PRODUCTLIST = new List<LotReplyTempleteBody.Product>()
                              {
                                  new LotReplyTempleteBody.Product()
                                  {
                                      PRODUCTNAME="",
                                      LOTNO="",
                                      RECIPENAME="",
                                      RECIPEPARAMLIST=new List<LotReplyTempleteBody.Product.RecipeParam>(){},
                                      MASKTITLE="",
                                      PRODUCTTYPE="",
                                      PRODUCTIONTYPE="",
                                      PROCESSOPERATIONNAME="",
                                      WORKORDER="",
                                      SLOTNO="",
                                      INPUTACTION="",
                                      BUFFERSLOTNO="",
                                      ROTATION="",
                                      INSPECTIONNAME=""
                                  }
                              }
                          });
                        Publish(replyMsg, basicProperties, msgHeader.TRANSACTIONID, true);
                    }
                    break;
                case "EapMesTrackOutRequest":
                    {
                        //解析Body的对象需标注xmlroot为Body
                        var requestBody = xmlDocument.GetBody<LotReplyTempleteBody>();
                        MessageFormat<LotReplyTempleteBody> replyMsg =
                          new("MesEapTrackOutRequestReply", new()
                          {
                              MACHINENAME = requestBody.MACHINENAME,
                              PORTNAME = requestBody.PORTNAME,
                              PORTTYPE = requestBody.PORTTYPE,
                              CARRIERNAME = requestBody.CARRIERNAME,
                              OPERATORID = requestBody.OPERATORID,
                              BATCHID = "",
                              SLOTMAP = "3",
                              PRODUCTLIST = new List<LotReplyTempleteBody.Product>()
                              {
                                  new LotReplyTempleteBody.Product()
                                  {
                                      PRODUCTNAME="",
                                      LOTNO="",
                                      RECIPENAME="",
                                      RECIPEPARAMLIST=new List<LotReplyTempleteBody.Product.RecipeParam>(){},
                                      MASKTITLE="",
                                      PRODUCTTYPE="",
                                      PRODUCTIONTYPE="",
                                      PROCESSOPERATIONNAME="",
                                      WORKORDER="",
                                      SLOTNO="",
                                      INPUTACTION="",
                                      BUFFERSLOTNO="",
                                      ROTATION="",
                                      INSPECTIONNAME=""
                                  }
                              }
                          });
                        Publish(replyMsg, basicProperties, msgHeader.TRANSACTIONID, true);
                    }
                    break;
                case "EapMesAreYouThereRequest":
                    {
                        //解析Body的对象需标注xmlroot为Body
                        var requestBody = xmlDocument.GetBody<EapMesAreYouThereRequest>();
                        MessageFormat<MesEapAreYouThereRequestReply> replyMsg =
                          new("MesEapAreYouThereRequestReply", new()
                          {
                              MACHINENAME = requestBody.MACHINENAME
                          });
                        Publish(replyMsg, basicProperties, msgHeader.TRANSACTIONID, true);
                    }
                    break;
                case "EapMesBufferSlotCheckRequest":
                    {
                        //解析Body的对象需标注xmlroot为Body
                        var requestBody = xmlDocument.GetBody<EapMesBufferSlotCheckRequest>();
                        MessageFormat<MesEapBufferSlotCheckRequestReply> replyMsg =
                          new("MesEapBufferSlotCheckRequestReply", new()
                          {
                              MACHINENAME = requestBody.MACHINENAME,
                              PORTNAME= requestBody.PORTNAME,
                              CARRIERNAME= requestBody.CARRIERNAME,
                              BUFFERSLOT = requestBody.BUFFERSLOT,
                          });
                        Publish(replyMsg, basicProperties, msgHeader.TRANSACTIONID, true);
                    }
                    break;
            }
        }
        catch(Exception ex)
        {
            LogHandler?.Invoke($"handle exception : {ex.Message},{ex.StackTrace},{ex.InnerException?.Message},{ex.InnerException?.StackTrace}");
        }
    }


    public void Publish<T>(MessageFormat<T> message, IBasicProperties basicProperties, string transActionId = "", bool returnOk = true) where T : class, new()
    {
        message.Header.TRANSACTIONID = transActionId != "" ? transActionId : Guid.NewGuid().ToString("N");
        message.Header.TIMESTAMP = DateTime.Now.ToString("yyyyMMddHHmmssfff");
        message.Header.TTLCALLBACK = "";
        message.Header.EVENTUSER = "";
        message.Header.ROUTINGKEY = "";
        message.Header.SENDEXCHANGE = "";
        message.Header.SOURCESUBJECTNAME = "";
        message.Header.TARGETSUBJECTNAME = "";
        message.Header.FACTORYNAME = "";

        if (!returnOk)
        {
            message.Return.RETURNCODE = 1;
            message.Return.RETURNMESSAGE = "NG";
        }

        var config = rabbitMqModule.GetProducterConfig();

        string msgStr = XmlHelper.XmlSerialize(message);
        
        rabbitMqModule.Publish(config.Exchange.ExchangeName, config.Queue.QueueName, config.RoutingKey, msgStr, false, basicProperties);
        LogHandler?.Invoke($"Publish : {msgStr}");
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    public void Send(string msgStr, bool isRpc)
    {
        var config = rabbitMqModule.GetProducterConfig();

        AsyncEventingBasicConsumer? consumer = rabbitMqModule.Publish(config.Exchange.ExchangeName, config.Queue.QueueName, config.RoutingKey, msgStr, true);
        if (consumer != null)
        {
            consumer.Received += (model, args) =>
            {
                var body = args.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                LogHandler?.Invoke($"Received : {message}");
                ReceivedLogHandler?.Invoke(message);

                // 若 ConsumerTag 为 RPC Queue 的，收到回复消息后取消消费
                ((AsyncEventingBasicConsumer)model).Model.BasicCancelNoWait(args.ConsumerTag);

                return Task.CompletedTask;
            };
        }
        LogHandler?.Invoke($"Publish :\r\n {msgStr}");
    }

    public void Close()
    {
        mesConsumer.Close();
        mesConsumer = null;
        rabbitMqModule.ChannelPool.Dispose();
        rabbitMqModule.ConnectionPool.Get().Close();
        rabbitMqModule.ConnectionPool.Dispose();
    }

    #region message body
    [XmlRoot(ElementName ="Body")]
    public class LotRequestTemplete
    {
        public string MACHINENAME { get; set; }
        public string PORTNAME { get; set; }

        public string PORTTYPE { get; set; }

        public string CARRIERNAME { get; set; }
        public string OPERATORID { get; set; }

    }
    [XmlRoot(ElementName = "Body")]
    public class LotReplyTempleteBody
    {
        public string MACHINENAME { get; set; }
        public string PORTNAME { get; set; }

        public string PORTTYPE { get; set; }

        public string CARRIERNAME { get; set; }
        public string OPERATORID { get; set; }
        public string BATCHID { get; set; }
        public string SLOTMAP { get; set; }

        [XmlArray]
        [XmlArrayItem(ElementName = "PRODUCT")]
        public List<Product> PRODUCTLIST { get; set; }

        public class Product
        {
            public string PRODUCTNAME { get; set; }
            public string LOTNO { get; set; }
            public string RECIPENAME { get; set; }

            [XmlArray]
            [XmlArrayItem(ElementName = "RECIPEPARAM")]
            public List<RecipeParam> RECIPEPARAMLIST { get; set; }

            public string MASKTITLE { get; set; }
            public string PRODUCTTYPE { get; set; }
            public string PRODUCTIONTYPE { get; set; }
            public string PROCESSOPERATIONNAME { get; set; }
            public string WORKORDER { get; set; }
            public string SLOTNO { get; set; }
            public string INPUTACTION { get; set; }
            public string BUFFERSLOTNO { get; set; }
            public string ROTATION { get; set; }
            public string INSPECTIONNAME { get; set; }


            public class RecipeParam
            {
                public string PARAMNAME { get; set; }
                public string PARAMVALUE { get; set; }
            }
        }

    }
    [XmlRoot(ElementName = "Body")]
    public class EapMesAreYouThereRequest
    {
        public string MACHINENAME { get; set; }
    }
    [XmlRoot(ElementName = "Body")]
    public class MesEapAreYouThereRequestReply
    {
        public string MACHINENAME { get; set; }
    }
    [XmlRoot(ElementName = "Body")]
    public class EapMesBufferSlotCheckRequest
    {
        public string MACHINENAME { get; set; }
        public string PORTNAME { get; set; }
        public string CARRIERNAME { get; set; }
        public string BUFFERSLOT { get; set; }
    }
    [XmlRoot(ElementName = "Body")]
    public class MesEapBufferSlotCheckRequestReply
    {
        public string MACHINENAME { get; set; }
        public string PORTNAME { get; set; }
        public string CARRIERNAME { get; set; }
        public string BUFFERSLOT { get; set; }
    }

    #endregion












}

