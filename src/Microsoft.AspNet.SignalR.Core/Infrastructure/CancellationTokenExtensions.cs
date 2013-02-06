// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Threading;

namespace Microsoft.AspNet.SignalR.Infrastructure
{
    internal static class CancellationTokenExtensions
    {
        public static IDisposable SafeRegister<T>(this CancellationToken cancellationToken, Action<T> callback, T state)
        {
            var callbackWrapper = new CancellationCallbackWrapper<T>(callback, state);
            
            try
            {
                CancellationTokenRegistration registration = cancellationToken.Register(tokenState =>
                {
                    ((CancellationCallbackWrapper<T>)tokenState).TryInvoke();
                },
                callbackWrapper,
                useSynchronizationContext: false);

                var disposeCancellationState = new DiposeCancellationState<T>(callbackWrapper, registration);

                return new DisposableAction(diposeState =>
                {
                    ((DiposeCancellationState<T>)diposeState).TryDispose();
                },
                disposeCancellationState);
            }
            catch (ObjectDisposedException)
            {
                callbackWrapper.TryInvoke();
            }

            // Noop
            return DisposableAction.Empty;
        }

        private class DiposeCancellationState<T>
        {
            private readonly CancellationCallbackWrapper<T> _callbackWrapper;
            private readonly CancellationTokenRegistration _registration;

            public DiposeCancellationState(CancellationCallbackWrapper<T> callbackWrapper, CancellationTokenRegistration registration)
            {
                _callbackWrapper = callbackWrapper;
                _registration = registration;
            }

            public void TryDispose()
            {
                if (_callbackWrapper.TrySetInvoked())
                {
                    // This normally waits until the callback is finished invoked but we don't care
                    _registration.Dispose();
                }
            }
        }
        
        private class CancellationCallbackWrapper<T>
        {
            private readonly Action<T> _callback;
            private readonly T _state;
            private int _callbackInvoked;

            public CancellationCallbackWrapper(Action<T> callback, T state)
            {
                _callback = callback;
                _state = state;
            }

            public bool TrySetInvoked()
            {
                return Interlocked.Exchange(ref _callbackInvoked, 1) == 0;
            }

            public void TryInvoke()
            {
                if (TrySetInvoked())
                {
                    _callback(_state);
                }
            }
        }
    }
}
