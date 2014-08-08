using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.Tracing;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;


namespace Signalr.MongoDb
{
    public class MongoMessageBus : ScaleoutMessageBus
    {

        private readonly MongoScaleoutConfiguration _config;

        private MongoCollection<MongoMessage> _collection;

        private readonly MongoUrl _url;

        private MongoServer _server;

        private Task _connectingTask;

        private readonly TraceSource _trace;
        private bool _connectionReady;

        private string _collectionName;

        private int _state;

        private readonly object _callbackLock = new object();

        public MongoMessageBus(IDependencyResolver resolver, MongoScaleoutConfiguration configuration)
            : base(resolver, configuration)
        {

            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }

            _config = configuration;
            _url = new MongoUrl(_config.ConnectionString);
            _trace = resolver.Resolve<ITraceManager>()["Signalr." + typeof(MongoMessageBus).Name];
            ConnectWithRetry();
        }


        private bool IsReady
        {
            get { return _connectionReady && _server != null && _server.State == MongoServerState.Connected; }
        }


        private void ConnectWithRetry()
        {

            Connect().ContinueWith(task =>
            {
                if (task.IsFaulted)
                {
                    _trace.TraceError("Error connecting to Mongo - " + task.Exception.GetBaseException());

                    if (_state == State.Disposing)
                    {
                        Shutdown();
                        return;
                    }

                    Task.Delay(_config.RetryDelay).Then(bus => bus.ConnectWithRetry(), this);
                }
                else
                {
                    var _oldState = Interlocked.CompareExchange(ref _state, State.Connected, State.Closed);
                    if (_oldState == State.Closed)
                    {
                        Open(0);
                    }
                    else if (_oldState == State.Disposing)
                    {
                        Shutdown();
                    }
                }
            },
            TaskContinuationOptions.ExecuteSynchronously);
        }

        private Task Connect()
        {

            var _tcs = new TaskCompletionSource<object>();

            if (IsReady || Interlocked.CompareExchange(ref _connectingTask, _tcs.Task, null) != null)
            {
                return TaskAsyncHelper.Empty;
            }
            try
            {
                if (_server == null)
                {

                    Task.Factory.StartNew(Init).ContinueWith((task) =>
                    {
                        if (task.IsFaulted)
                        {
                            _tcs.SetException(task.Exception);
                            return;
                        }
                        if (task.IsCanceled)
                        {
                            _tcs.SetCanceled();
                            return;
                        }
                        //ready to send notifications
                        _tcs.SetResult(null);
                        _connectionReady = true;
                        //setup the receiving loop
                        //this is a blocking call while running
                        Receiving();
                    });

                }

                return _connectingTask.Catch();
            }
            catch (Exception ex)
            {
                return TaskAsyncHelper.FromError(ex);
            }

        }

        private void Init()
        {
            _server = new MongoClient(_url).GetServer();
            var _db = _server.GetDatabase(_url.DatabaseName);

            _trace.TraceInformation("Opened Mongo database {0}", _url.DatabaseName);

            _collectionName = GetCollectionName<MongoMessage>();

            //create the collection if it doesn't exist
            if (!_db.CollectionExists(_collectionName))
            {
                _db.CreateCollection(_collectionName, CollectionOptions.SetAutoIndexId(true).SetCapped(true).SetMaxSize(_config.CollectionMaxSize).SetMaxDocuments(_config.MaxDocuments));

                ////Insert an empty document as without this 'cursor.IsDead' is always true
                _db.GetCollection<MongoMessage>(_collectionName).Insert(new MongoMessage(new byte[] { }) { Status = 1 });
            }

            _collection = _db.GetCollection<MongoMessage>(_collectionName);

            _trace.TraceInformation("Opened Mongo collection {0}", _collectionName);

            if (!_collection.IsCapped())
            {
                _trace.TraceInformation("Existing Mongo collection is not capped collection {0}", _collectionName);
                throw new MongoConnectionException(string.Format("MongoCollection {0} must be capped", _collectionName));
            }
        }

        protected override Task Send(int streamIndex, IList<Message> messages)
        {
            _trace.TraceVerbose("Send called with stream index {0}.", streamIndex);

            var _msg = new MongoMessage(streamIndex, messages);

            if (IsReady)
            {
                return Task.Factory.StartNew(() => _collection.Insert(_msg)).Catch();
            }

            return Connect().Then(() => Task.Factory.StartNew(() => _collection.Insert(_msg)));
        }


        //http://stackoverflow.com/questions/20700161/mongodb-2-4-8-capped-collection-and-tailable-cursor-consuming-all-memory
        private void Receiving()
        {

            try
            {
                
                BsonValue _lastId = BsonMinKey.Value;
                _trace.TraceInformation("Found collection {0} start point {1}", _collectionName, _lastId);
                while (true)
                {

                    IMongoQuery _query = Query.And(
                        Query<MongoMessage>.GT(x => x.Id, _lastId),
                        Query<MongoMessage>.EQ(x => x.Status, 0)
                        );

                    var _data = _collection.Find(_query)
                        .SetFlags(QueryFlags.TailableCursor | QueryFlags.NoCursorTimeout | QueryFlags.AwaitData);

                    var _cursor = new MongoCursorEnumerator<MongoMessage>(_data);

                    while (true)
                    {
                        if (_cursor.MoveNext())
                        {
                            var _msg = _cursor.Current;
                            Process(_msg);
                            _lastId = _msg.Id;
                            var _update = Update<MongoMessage>.Set(x => x.Status, 1);
                            _collection.Update(Query<MongoMessage>.EQ(x => x.Id, _msg.Id), _update);

                        }
                        else
                        {
                            if (_cursor.IsDead) break;

                            if (!_cursor.IsServerAwaitCapable) Thread.Sleep(TimeSpan.FromMilliseconds(150));
                        }
                    }
                }


            }
            catch (IOException ex)
            {
                //reset the connection to force a reconnect
                _connectionReady = false;
                _connectingTask = null;
            }
        }


        private void Process(MongoMessage _msg)
        {
            try
            {
                lock (_callbackLock)
                {
                    var _scaleoutMessage = ScaleoutMessage.FromBytes(_msg.Value);
                    OnReceived(_msg.StreamIndex, (ulong)_msg.Id.CreationTime.Ticks, _scaleoutMessage);
                }

            }
            catch (Exception ex)
            {
                _trace.TraceInformation("Error adding message to InProcessBus. EventKey={0}, Value={1}. Error={2}, Stack={3}",
                    _msg.Id, _msg.Value, ex.Message, ex.StackTrace);
                Debug.WriteLine(ex.Message);

                OnError(0, ex);
            }
        }



        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                var oldState = Interlocked.Exchange(ref _state, State.Disposing);
                switch (oldState)
                {
                    case State.Connected:
                    case State.Closed:
                        Shutdown();
                        break;
                    case State.Disposed:
                        Interlocked.Exchange(ref _state, State.Disposed);
                        break;
                }
            }


            base.Dispose(disposing);
        }

        private void Shutdown()
        {
            _trace.TraceInformation("Shutdown...");

            if (_server != null)
            {
                _server.Disconnect();
            }

            Interlocked.Exchange(ref _state, State.Disposed);

            _trace.TraceInformation("Goodbye...");
        }



        private string GetCollectionName<T>()
        {
            var _att = Attribute.GetCustomAttribute(typeof(T), typeof(CollectionName));
            string _collectionName = _att != null ? ((CollectionName)_att).Name : typeof(T).Name;

            if (string.IsNullOrEmpty(_collectionName))
            {
                throw new ArgumentException("Collection name cannot be empty for this entity");
            }
            return _collectionName;
        }
        private static class State
        {
            public const int Closed = 0;
            public const int Connected = 1;
            public const int Disposing = 2;
            public const int Disposed = 3;
        }
    }


}
