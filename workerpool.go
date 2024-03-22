package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
)

const (
	// 如果工人至少在这段时间内处于闲置状态，那么就停止一名工人的工作。
	idleTimeout = 2 * time.Second
)

// New creates and starts a pool of worker goroutines.
//
// MaxWorkers参数指定可以并发执行任务的最大工人数量。
// 当没有传入的任务时，工人会逐渐停止，直到没有剩余的工人为止。
func New(maxWorkers int) *WorkerPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	pool := &WorkerPool{
		maxWorkers:  maxWorkers,
		taskQueue:   make(chan func()),
		workerQueue: make(chan func()),
		stopSignal:  make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}

	// Start the task dispatcher.
	go pool.dispatch()

	return pool
}

// WorkerPool is a collection of goroutines, where the number of concurrent
// goroutines processing requests does not exceed the specified maximum.
type WorkerPool struct {
	maxWorkers   int
	taskQueue    chan func()
	workerQueue  chan func()
	stoppedChan  chan struct{}
	stopSignal   chan struct{}
	waitingQueue deque.Deque[func()]
	stopLock     sync.Mutex
	stopOnce     sync.Once
	stopped      bool
	waiting      int32
	wait         bool
}

// Size returns the maximum number of concurrent workers.
func (p *WorkerPool) Size() int {
	return p.maxWorkers
}

// Stop将停止工作池并仅等待当前正在运行的任务完成。
// 当前未运行的挂起任务将被放弃。在调用Stop之后，任务不得提交到工作池。
//
// 由于创建工作池至少会启动一个goroutine，因此对于调度程序来说，当不再需要工作池时，应该调用Stop()或StopWait()。
func (p *WorkerPool) Stop() {
	p.stop(false)
}

// StopWait停止工作池，并等待所有排队的任务任务完成。
// 不能提交额外任务，但是在此函数返回之前，工人执行所有挂起的任务。
func (p *WorkerPool) StopWait() {
	p.stop(true)
}

// Stopped returns true if this worker pool has been stopped.
func (p *WorkerPool) Stopped() bool {
	p.stopLock.Lock()
	defer p.stopLock.Unlock()
	return p.stopped
}

// Submit enqueues a function for a worker to execute.
//
// 任务函数所需的任何外部值都必须在闭包中捕获。任何返回值都应该通过任务函数闭包中捕获的通道返回。
// 
// 无论提交的任务数量如何，Submit都不会阻塞。
// 每个任务都会立即分配给可用的工作人员或新启动的工作人员。
// 如果没有可用的工作者，并且已经创建了最大数量的工作者，则该任务将被放入等待队列中。
// 
// 当等待队列中有任务时，任何额外的新任务都会放置在等待队列中。
// 当工作线程可用时，将从等待队列中移除任务。
// 
// 只要没有新任务到达，每个时间段就会关闭一个可用的工作线程，直到没有更多的闲置工作线程为止。
// 由于开始新的goroutines的时间并不重要，因此无需无限期地保留闲置的工作线程。
func (p *WorkerPool) Submit(task func()) {
	if task != nil {
		p.taskQueue <- task
	}
}

// SubmitWait将给定函数排入队列并等待其执行。
func (p *WorkerPool) SubmitWait(task func()) {
	if task == nil {
		return
	}
	doneChan := make(chan struct{})
	p.taskQueue <- func() {
		task()
		// 关闭后的通道有以下特点：
		// 1.对一个关闭的通道再发送值就会导致panic。
		// 2.对一个关闭的通道进行接收会一直获取值直到通道为空。
		// 3.对一个关闭的并且没有值的通道执行接收操作会得到对应类型的零值。
		// 4.关闭一个已经关闭的通道会导致panic。
		close(doneChan)
	}
	// 如果接收操作先执行，接收方的goroutine将阻塞，直到另一个goroutine在该通道上发送一个值或者这个通道已被关闭。
	<-doneChan
}

// WaitingQueueSize returns the count of tasks in the waiting queue.
func (p *WorkerPool) WaitingQueueSize() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// Pause causes all workers to wait on the given Context, thereby making them
// unavailable to run tasks. Pause returns when all workers are waiting. Tasks
// can continue to be queued to the workerpool, but are not executed until the
// Context is canceled or times out.
// Pause会导致所有工作线程在给定的上下文上等待，从而使他们无法运行任务。
// 当所有工作线程都在等待时，Pause返回。
// 任务可以继续排队到workerpool，但在上下文被取消或超时之前不会执行。
//
// 当workpool已经暂停时调用Pause会导致Pause等待，直到所有先前的pause被取消。
// 这使一个goroutine能够在其他Goroutines停止Pause后立即控制Pause和停止Pause游泳池。
//
// When the workerpool is stopped, workers are unpaused and queued tasks are
// executed during StopWait.
func (p *WorkerPool) Pause(ctx context.Context) {
	p.stopLock.Lock()
	defer p.stopLock.Unlock()
	if p.stopped {
		return
	}
	ready := new(sync.WaitGroup)
	ready.Add(p.maxWorkers)
	for i := 0; i < p.maxWorkers; i++ {
		p.Submit(func() {
			ready.Done()
			select {
			case <-ctx.Done():
			case <-p.stopSignal:
			}
		})
	}
	// Wait for workers to all be paused
	ready.Wait()
}

// dispatch sends the next queued task to an available worker.
func (p *WorkerPool) dispatch() {
	defer close(p.stoppedChan)
	timeout := time.NewTimer(idleTimeout)
	var workerCount int
	var idle bool
	var wg sync.WaitGroup

Loop:
	for {
		// As long as tasks are in the waiting queue, incoming tasks are put
		// into the waiting queue and tasks to run are taken from the waiting
		// queue. Once the waiting queue is empty, then go back to submitting
		// incoming tasks directly to available workers.
		if p.waitingQueue.Len() != 0 {
			if !p.processWaitingQueue() {
				break Loop
			}
			continue
		}

		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				break Loop
			}
			// Got a task to do.
			select {
			case p.workerQueue <- task:
			default:
				// Create a new worker, if not at max.
				if workerCount < p.maxWorkers {
					wg.Add(1)
					go worker(task, p.workerQueue, &wg)
					workerCount++
				} else {
					// Enqueue task to be executed by next available worker.
					p.waitingQueue.PushBack(task)
					atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
				}
			}
			idle = false
		case <-timeout.C:
			// Timed out waiting for work to arrive. Kill a ready worker if
			// pool has been idle for a whole timeout.
			if idle && workerCount > 0 {
				if p.killIdleWorker() {
					workerCount--
				}
			}
			idle = true
			timeout.Reset(idleTimeout)
		}
	}

	// If instructed to wait, then run tasks that are already queued.
	if p.wait {
		p.runQueuedTasks()
	}

	// Stop all remaining workers as they become ready.
	for workerCount > 0 {
		p.workerQueue <- nil
		workerCount--
	}
	wg.Wait()

	timeout.Stop()
}

// worker executes tasks and stops when it receives a nil task.
func worker(task func(), workerQueue chan func(), wg *sync.WaitGroup) {
	for task != nil {
		task()
		task = <-workerQueue
	}
	wg.Done()
}

// stop tells the dispatcher to exit, and whether or not to complete queued
// tasks.
func (p *WorkerPool) stop(wait bool) {
	p.stopOnce.Do(func() {
		// Signal that workerpool is stopping, to unpause any paused workers.
		close(p.stopSignal)
		// Acquire stopLock to wait for any pause in progress to complete. All
		// in-progress pauses will complete because the stopSignal unpauses the
		// workers.
		p.stopLock.Lock()
		// The stopped flag prevents any additional paused workers. This makes
		// it safe to close the taskQueue.
		p.stopped = true
		p.stopLock.Unlock()
		p.wait = wait
		// Close task queue and wait for currently running tasks to finish.
		close(p.taskQueue)
	})
	<-p.stoppedChan
}

// processWaitingQueue puts new tasks onto the the waiting queue, and removes
// tasks from the waiting queue as workers become available. Returns false if
// worker pool is stopped.
func (p *WorkerPool) processWaitingQueue() bool {
	select {
	case task, ok := <-p.taskQueue:
		if !ok {
			return false
		}
		p.waitingQueue.PushBack(task)
	case p.workerQueue <- p.waitingQueue.Front():
		// A worker was ready, so gave task to worker.
		p.waitingQueue.PopFront()
	}
	atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
	return true
}

func (p *WorkerPool) killIdleWorker() bool {
	select {
	case p.workerQueue <- nil:
		// Sent kill signal to worker.
		return true
	default:
		// No ready workers. All, if any, workers are busy.
		return false
	}
}

// runQueuedTasks removes each task from the waiting queue and gives it to
// workers until queue is empty.
func (p *WorkerPool) runQueuedTasks() {
	for p.waitingQueue.Len() != 0 {
		// A worker is ready, so give task to worker.
		p.workerQueue <- p.waitingQueue.PopFront()
		atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
	}
}
