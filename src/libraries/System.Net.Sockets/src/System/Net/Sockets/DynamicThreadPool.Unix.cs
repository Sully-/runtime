// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace System.Net.Sockets
{
    [EventSource(Name = "System.Net.Sockets.DynamicThreadPool")]
    internal class DynamicThreadPoolEventSource : EventSource
    {
        private static DynamicThreadPoolEventSource? s_dynamicThreadPoolEventSource;
        private PollingCounter? _dynamicThreadPoolQueueCounter;
        public static void Initialize()
        {
            s_dynamicThreadPoolEventSource = new DynamicThreadPoolEventSource();
        }
        protected override void OnEventCommand(EventCommandEventArgs command)
        {
            if (command.Command == EventCommand.Enable)
            {
                _dynamicThreadPoolQueueCounter ??= new PollingCounter("dynamic-threadpool-queue-length", this, () => DynamicThreadPool.IOThreadPool.pendingWorkItemCount()) { DisplayName = "Dynamic ThreadPool Queue Length" };
            }
        }
    }
    internal class DynamicThreadPool
    {
        internal static readonly DynamicThreadPool IOThreadPool = new DynamicThreadPool(Environment.ProcessorCount * 2);

        private long _createdWorkers;
        public long CreatedWorkers => _createdWorkers;

        private int _aliveWorkers;
        public int AliveWorkers => _aliveWorkers;

        private int _reserveWorkers;
        public int ReserveWorkers => _reserveWorkers;

        private const int ReserveWorkersLimit = 5;
        private const int WorkerTimeout = 20 * 1000;
        private const int StartWorkerDelay = 500;

        private readonly ConcurrentQueue<(WaitCallback callback, object state)> _queue;
        private readonly ConcurrentStack<Worker> _pendingWorkers;

        public DynamicThreadPool(int minSize)
        {
            _queue = new ConcurrentQueue<(WaitCallback callback, object state)>();
            _pendingWorkers = new ConcurrentStack<Worker>();
            DynamicThreadPoolEventSource.Initialize();

            new Thread(GateThread) { IsBackground = true, Name = "DynamicThreadPoolGate" }.Start();

            for (int i = 0; i < minSize; i++)
            {
                StartWorker(Timeout.Infinite);
            }
        }

        public void QueueWorkItem(IThreadPoolWorkItem item)
        {
            QueueWorkItem(i => ((IThreadPoolWorkItem)i!).Execute(), item);
        }

        public void QueueWorkItem(WaitCallback callback, object state)
        {
            _queue.Enqueue((callback, state));
            TryWakeUpWorker();
        }

        private bool TryWakeUpWorker()
        {
            if (_queue.IsEmpty)
            {
                return true;
            }

            // There's something in the queue. Let's try to wake up a worker.
            while (_pendingWorkers.TryPop(out var worker))
            {
                if (worker.TryWakeUp())
                {
                    // At least one worker is awake, the action will be dequeued eventually
                    return true;
                }

                if (_queue.IsEmpty)
                {
                    // No need to wake up anyone, as the items already got processed
                    return true;
                }
            }

            // There's still something in the queue, and we failed to wake up a worker
            return false;
        }

        private void GateThread()
        {
            // The gate thread adds new worker to the thread pool when needed
            while (true)
            {
                Thread.Sleep(StartWorkerDelay);

                if (!TryWakeUpWorker())
                {
                    StartWorker(WorkerTimeout);
                }
            }
        }

        private void StartWorker(int timeout)
        {
            Interlocked.Increment(ref _createdWorkers);
            new Thread(() => WorkerThread(timeout)) { IsBackground = true, Name = "DynamicThreadPoolWorker" }.Start();
        }

        private void WorkerThread(int timeout)
        {
            Interlocked.Increment(ref _aliveWorkers);
            var worker = new Worker();

            while (true)
            {
                // Execute items while the queue is not empty
                if (TryExecuteItem())
                {
                    continue;
                }

                // Nothing left in the queue, prepare to sleep
                worker.Event.Reset();
                _pendingWorkers.Push(worker);
                if (worker.Event.Wait(timeout))
                {
                    continue;
                }

                // Nobody wakes us up for a long time. Let's decide if the thread pool should be scaled down
                // Workers created at startup (minSize) will never reach this line as they have infinite wait timeout
                var reserveCount = Interlocked.Increment(ref _reserveWorkers);
                if (reserveCount <= ReserveWorkersLimit)
                {
                    // Too early to scale down. This worker will wait "in reserve" until woken up
                    worker.Event.Wait();
                    Interlocked.Decrement(ref _reserveWorkers);
                    continue;
                }

                // ReserveWorkersLimit is reached, we can try to exit
                Interlocked.Decrement(ref _reserveWorkers);
                if (worker.TryExit())
                {
                    Interlocked.Decrement(ref _aliveWorkers);
                    return;
                }
            }
        }

        private bool TryExecuteItem()
        {
            if (_queue.TryDequeue(out var item))
            {
                try
                {
                    // TODO : add an event here. Curretn activity ID, current thread id, current task id
                    item.callback(item.state);
                }
                catch (Exception)
                {
                }

                return true;
            }

            return false;
        }

        public int pendingWorkItemCount()
        {
            return _queue.Count;
        }

        internal class Worker
        {
            public readonly ManualResetEventSlim Event = new ManualResetEventSlim(false);
            private bool _hasExited;

            public bool TryWakeUp()
            {
                lock (Event)
                {
                    if (!_hasExited)
                    {
                        Event.Set();
                        return true;
                    }
                }

                return false;
            }

            public bool TryExit()
            {
                lock (Event)
                {
                    if (!Event.IsSet)
                    {
                        _hasExited = true;
                        Event.Dispose();
                        return true;
                    }
                }

                return false;
            }
        }
    }
}
