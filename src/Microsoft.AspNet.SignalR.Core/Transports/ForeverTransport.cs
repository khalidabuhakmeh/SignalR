// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR.Hosting;
using Microsoft.AspNet.SignalR.Infrastructure;
using Microsoft.AspNet.SignalR.Json;
using Microsoft.AspNet.SignalR.Tracing;

namespace Microsoft.AspNet.SignalR.Transports
{
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable", Justification = "The disposer is an optimization")]
    public abstract class ForeverTransport : TransportDisconnectBase, ITransport
    {
        private readonly IPerformanceCounterManager _counters;
        private IJsonSerializer _jsonSerializer;
        private string _lastMessageId;
        private Disposer _requestDisposer;

        private readonly Func<object, Task> _send;
        private readonly Action<AggregateException> _sendError;

        private const int MaxMessages = 10;

        protected ForeverTransport(HostContext context, IDependencyResolver resolver)
            : this(context,
                   resolver.Resolve<IJsonSerializer>(),
                   resolver.Resolve<ITransportHeartbeat>(),
                   resolver.Resolve<IPerformanceCounterManager>(),
                   resolver.Resolve<ITraceManager>())
        {
        }

        protected ForeverTransport(HostContext context,
                                   IJsonSerializer jsonSerializer,
                                   ITransportHeartbeat heartbeat,
                                   IPerformanceCounterManager performanceCounterWriter,
                                   ITraceManager traceManager)
            : base(context, heartbeat, performanceCounterWriter, traceManager)
        {
            _jsonSerializer = jsonSerializer;
            _counters = performanceCounterWriter;

            _send = PerformSend;
            _sendError = OnSendError;
        }

        protected string LastMessageId
        {
            get
            {
                if (_lastMessageId == null)
                {
                    _lastMessageId = Context.Request.QueryString["messageId"];
                }

                return _lastMessageId;
            }
        }

        protected IJsonSerializer JsonSerializer
        {
            get { return _jsonSerializer; }
        }

        internal TaskCompletionSource<object> InitializeTcs { get; set; }

        protected virtual void OnSending(string payload)
        {
            Heartbeat.MarkConnection(this);
        }

        protected virtual void OnSendingResponse(PersistentResponse response)
        {
            Heartbeat.MarkConnection(this);
        }

        public Func<string, Task> Received { get; set; }

        public Func<Task> TransportConnected { get; set; }

        public Func<Task> Connected { get; set; }

        public Func<Task> Reconnected { get; set; }

        // Unit testing hooks
        internal Action AfterReceive;
        internal Action BeforeCancellationTokenCallbackRegistered;
        internal Action BeforeReceive;
        internal Action<Exception> AfterRequestEnd;

        protected override void InitializePersistentState()
        {
            // PersistentConnection.OnConnected must complete before we can write to the output stream,
            // so clients don't indicate the connection has started too early.
            InitializeTcs = new TaskCompletionSource<object>();
            WriteQueue = new TaskQueue(InitializeTcs.Task);

            _requestDisposer = new Disposer();

            base.InitializePersistentState();
        }

        protected Task ProcessRequestCore(ITransportConnection connection)
        {
            Connection = connection;

            if (Context.Request.Url.LocalPath.EndsWith("/send", StringComparison.OrdinalIgnoreCase))
            {
                return ProcessSendRequest();
            }
            else if (IsAbortRequest)
            {
                return Connection.Abort(ConnectionId);
            }
            else
            {
                InitializePersistentState();

                if (IsConnectRequest)
                {
                    if (Connected != null)
                    {
                        // Return a task that completes when the connected event task & the receive loop task are both finished
                        bool newConnection = Heartbeat.AddConnection(this);

                        // The connected callback
                        Func<Task> connected = () =>
                        {
                            if (newConnection)
                            {
                                return Connected().Then(() => _counters.ConnectionsConnected.Increment());
                            }
                            return TaskAsyncHelper.Empty;
                        };

                        return TaskAsyncHelper.Interleave(ProcessReceiveRequestWithoutTracking, connected, connection, Completed);
                    }

                    return ProcessReceiveRequest(connection);
                }

                if (Reconnected != null)
                {
                    // Return a task that completes when the reconnected event task & the receive loop task are both finished
                    Func<Task> reconnected = () => Reconnected().Then(() => _counters.ConnectionsReconnected.Increment());
                    return TaskAsyncHelper.Interleave(ProcessReceiveRequest, reconnected, connection, Completed);
                }

                return ProcessReceiveRequest(connection);
            }
        }

        public virtual Task ProcessRequest(ITransportConnection connection)
        {
            return ProcessRequestCore(connection);
        }

        public abstract Task Send(PersistentResponse response);

        public virtual Task Send(object value)
        {
            return EnqueueOperation(_send, value);
        }

        protected internal virtual Task InitializeResponse(ITransportConnection connection)
        {
            return TaskAsyncHelper.Empty;
        }

        protected internal override Task EnqueueOperation(Func<object, Task> writeAsync, object state)
        {
            Task task = base.EnqueueOperation(writeAsync, state);

            // If PersistentConnection.OnConnected has not completed (as indicated by InitializeTcs),
            // the queue will be blocked to prevent clients from prematurely indicating the connection has
            // started, but we must keep receive loop running to continue processing commands and to
            // prevent deadlocks caused by waiting on ACKs.
            if (InitializeTcs == null || InitializeTcs.Task.IsCompleted)
            {
                return task;
            }

            return TaskAsyncHelper.Empty;
        }

        protected override void ReleaseRequest()
        {
            _requestDisposer.Dispose();
        }

        private Task ProcessSendRequest()
        {
            string data = Context.Request.Form["data"];

            if (Received != null)
            {
                return Received(data);
            }

            return TaskAsyncHelper.Empty;
        }

        private Task ProcessReceiveRequest(ITransportConnection connection, Func<Task> postReceive = null)
        {
            Heartbeat.AddConnection(this);
            return ProcessReceiveRequestWithoutTracking(connection, postReceive);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are flowed to the caller.")]
        private Task ProcessReceiveRequestWithoutTracking(ITransportConnection connection, Func<Task> postReceive = null)
        {
            postReceive = postReceive ?? new Func<Task>(() => TaskAsyncHelper.Empty);

            Func<Task> afterReceive = () =>
            {
                var series = new Func<object, Task>[] 
                { 
                    OnTransportConnected,
                    state => InitializeResponse((ITransportConnection)state),
                    state => ((Func<Task>)state).Invoke()
                };

                return TaskAsyncHelper.Series(series, new object[] { null, connection, postReceive });
            };

            return ProcessMessages(connection, afterReceive);
        }

        private Task OnTransportConnected(object state)
        {
            if (TransportConnected != null)
            {
                return TransportConnected().Catch(_counters.ErrorsAllTotal, _counters.ErrorsAllPerSec);
            }

            return TaskAsyncHelper.Empty;
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "The object is disposed otherwise")]
        private Task ProcessMessages(ITransportConnection connection, Func<Task> postReceive)
        {
            var processMessagesTcs = new TaskCompletionSource<object>();

            var lifetime = new RequestLifetime(OnConnectionEnd, processMessagesTcs);

            var disposable = new DisposableAction(state =>
                                                  ((RequestLifetime)state).Complete(error: null),
                                                  lifetime);

            _requestDisposer.Set(disposable);

            ProcessMessages(connection, postReceive, lifetime);

            return processMessagesTcs.Task;
        }

        private void OnConnectionEnd(Exception error, TaskCompletionSource<object> taskCompletionSource)
        {
            Trace.TraceEvent(TraceEventType.Verbose, 0, "DrainWrites(" + ConnectionId + ")");

            var drainContext = new DrainContext(CompleteRequest, taskCompletionSource, error, Trace, ConnectionId);

            // Drain the task queue for pending write operations so we don't end the request and then try to write
            // to a corrupted request object.
            WriteQueue.Drain().Catch().Finally(state =>
            {
                ((DrainContext)state).Complete();
            },
            drainContext);

            if (AfterRequestEnd != null)
            {
                AfterRequestEnd(error);
            }
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "The object is disposed otherwise")]
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are flowed to the caller.")]
        private void ProcessMessages(ITransportConnection connection, Func<Task> postReceive, RequestLifetime lifetime)
        {
            IDisposable subscription = null;
            var disposer = new Disposer();
            var messageContext = new MessageContext(disposer, lifetime);

            if (BeforeReceive != null)
            {
                BeforeReceive();
            }

            try
            {
                subscription = connection.Receive(LastMessageId,
                                                  OnMessageReceived,
                                                  MaxMessages,
                                                  messageContext);
            }
            catch (Exception ex)
            {
                // Set the tcs so that the task queue isn't waiting forever
                InitializeTcs.TrySetResult(null);

                lifetime.Complete(ex);

                return;
            }

            if (AfterReceive != null)
            {
                AfterReceive();
            }

            postReceive().Catch(OnPostReceiveError, lifetime)
                         .ContinueWith(InitializeTcs);

            if (BeforeCancellationTokenCallbackRegistered != null)
            {
                BeforeCancellationTokenCallbackRegistered();
            }

            // This has to be done last incase it runs synchronously.
            IDisposable registration = ConnectionEndToken.SafeRegister(state =>
            {
                Trace.TraceEvent(TraceEventType.Verbose, 0, "Cancel(" + ConnectionId + ")");

                state.Dispose();
            },
            subscription);

            disposer.Set(registration);
        }

        private Task<bool> OnMessageReceived(PersistentResponse response, object state)
        {
            var context = (MessageContext)state;

            response.TimedOut = IsTimedOut;

            // If we're telling the client to disconnect then clean up the instantiated connection.
            if (response.Disconnect)
            {
                // Send the response before removing any connection data
                return Send(response).Then(c => OnDisconnectMessage(c), context)
                                     .Then(() => TaskAsyncHelper.False);
            }
            else if (response.TimedOut ||
                     response.Aborted ||
                     ConnectionEndToken.IsCancellationRequested)
            {
                context.Disposer.Dispose();

                if (response.Aborted)
                {
                    // If this was a clean disconnect raise the event.
                    OnDisconnect();
                }

                context.Lifetime.Complete(error: null);

                return TaskAsyncHelper.False;
            }

            return Send(response).Then(() => TaskAsyncHelper.True)
                                 .Catch(_sendError);
        }

        private void OnDisconnectMessage(MessageContext context)
        {
            context.Disposer.Dispose();

            // Remove connection without triggering disconnect
            Heartbeat.RemoveConnection(this);

            context.Lifetime.Complete(error: null);
        }

        private Task PerformSend(object state)
        {
            Context.Response.ContentType = JsonUtility.JsonMimeType;

            JsonSerializer.Serialize(state, OutputWriter);
            OutputWriter.Flush();

            return Context.Response.End().Catch(IncrementErrors);
        }

        private void OnSendError(AggregateException ex)
        {
            IncrementErrors(ex);

            Trace.TraceEvent(TraceEventType.Error, 0, "Send failed for {0} with: {1}", ConnectionId, ex.GetBaseException());
        }

        private void OnPostReceiveError(AggregateException ex, object state)
        {
            Trace.TraceEvent(TraceEventType.Error, 0, "Failed post receive for {0} with: {1}", ConnectionId, ex.GetBaseException());

            ((RequestLifetime)state).Complete(ex);

            _counters.ErrorsAllTotal.Increment();
            _counters.ErrorsAllPerSec.Increment();
        }

        private class MessageContext
        {
            public MessageContext(Disposer disposer, RequestLifetime lifetime)
            {
                Disposer = disposer;
                Lifetime = lifetime;
            }

            public Disposer Disposer { get; private set; }
            public RequestLifetime Lifetime { get; private set; }
        }

        private class RequestLifetime
        {
            private Action<Exception, TaskCompletionSource<object>> _endRequest;
            private readonly TaskCompletionSource<object> _tcs;

            public RequestLifetime(Action<Exception, TaskCompletionSource<object>> endRequest, TaskCompletionSource<object> tcs)
            {
                _endRequest = endRequest;
                _tcs = tcs;
            }

            public void Complete(Exception error)
            {
                // Only allow invoking this once
                Interlocked.Exchange(ref _endRequest, (ex, state) => { }).Invoke(error, _tcs);
            }
        }

        private class DrainContext
        {
            private readonly Action _completeRequest;
            private readonly TaskCompletionSource<object> _processMessagesTcs;
            private readonly Exception _error;
            private readonly TraceSource _trace;
            private readonly string _connectionId;

            public DrainContext(Action completeRequest,
                                TaskCompletionSource<object> processMessagesTcs,
                                Exception error,
                                TraceSource trace,
                                string connectionId)
            {
                _completeRequest = completeRequest;
                _processMessagesTcs = processMessagesTcs;
                _error = error;
                _trace = trace;
                _connectionId = connectionId;
            }

            public void Complete()
            {
                if (_error != null)
                {
                    _processMessagesTcs.TrySetException(_error);
                }
                else
                {
                    _processMessagesTcs.TrySetResult(null);
                }

                _completeRequest();

                _trace.TraceInformation("EndRequest(" + _connectionId + ")");
            }
        }
    }
}
