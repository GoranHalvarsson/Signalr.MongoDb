using System;
using System.Collections.Generic;
using Microsoft.AspNet.SignalR.Messaging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Signalr.MongoDb
{
    [CollectionName("messagebus")]
    public class MongoMessage
    {

        /*[BsonRepresentation(BsonType.ObjectId)]*/
        [BsonId]
        public ObjectId Id { get; set; }
        
        [BsonElement("i")]
        public int StreamIndex { get; set; }

        [BsonElement("v")]
        public byte[] Value { get; set; }

        [BsonDefaultValue(0)]
        [BsonElement("t")]
        public byte Status { get; set; }

        [BsonIgnore]
        public DateTime Created
        {
            get
            {
                //if we retrieved the MongoMessage - then it has a ObjectId - so read the date/time
               // return Id != null ? ObjectId.Parse(Id).CreationTime : DateTime.MinValue;

                return Id.CreationTime;
            }
        }





        public static byte[] ToBytes(IList<Message> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }

            /* using (var ms = new MemoryStream())
             {
                 var binaryWriter = new BinaryWriter(ms);

                 var scaleoutMessage = new ScaleoutMessage(messages);
                 var buffer = scaleoutMessage.ToBytes();

                 binaryWriter.Write(buffer.Length);
                 binaryWriter.Write(buffer);

                 return ms.ToArray();
             }*/

            var _message = new ScaleoutMessage(messages);
            return _message.ToBytes();
        }

        /* public static MongoMessage FromBytes(byte[] data)
         {
             using (var stream = new MemoryStream(data))
             {
                 var message = new MongoMessage();

               /*  // read message id from memory stream until SPACE character
                 var messageIdBuilder = new StringBuilder();
                 do
                 {
                     // it is safe to read digits as bytes because they encoded by single byte in UTF-8
                     int charCode = stream.ReadByte();
                     if (charCode == -1)
                     {
                         throw new EndOfStreamException();
                     }
                     char c = (char)charCode;
                     if (c == ' ')
                     {
                         message.Id = ulong.Parse(messageIdBuilder.ToString(), CultureInfo.InvariantCulture);
                         messageIdBuilder = null;
                     }
                     else
                     {
                         messageIdBuilder.Append(c);
                     }
                 }
                 while (messageIdBuilder != null);#1#

                 var binaryReader = new BinaryReader(stream);
                 int count = binaryReader.ReadInt32();
                 byte[] buffer = binaryReader.ReadBytes(count);

                 message = ScaleoutMessage.FromBytes(buffer);
                 return message;
             }
         }*/

        public static ScaleoutMessage FromBytes(MongoMessage msg)
        {
            return ScaleoutMessage.FromBytes(msg.Value);
        }

        public MongoMessage(IList<Message> messages)
            : this(ToBytes(messages))
        {
           
        }

        public MongoMessage(int streamIndex, IList<Message> messages)
            : this(ToBytes(messages))
        {
            StreamIndex = streamIndex;

        }

        public MongoMessage(byte[] value)
        {
            /*Id = ObjectId.GenerateNewId().ToString();ConnectionId = connectionId;
            EventKey = eventKey;*/
            Value = value;
        }
        
        
    }
}
