---
title: informer源码分析- work queue
summary: 
tags:
  - kubernetes
date: 2025-06-01
toc: true
# external_link: http://github.com
---
<!-- {{< toc mobile_only=false is_open=true >}} -->
# 背景

实现自定义controller时，会创建workqueue队列来处理eventhandler添加进来的对象，试着通过阅读源码的方式来看看这个队列和普通队列有什么不同，是怎么进行限速的。通过Queue->DelayingQueue->RateLimiterQueue递进的顺序来进行分析

## 1.Queue的实现

Queue的接口定义：

```go
type Interface interface {
	Add(item interface{})
	Len() int
	Get() (item interface{}, shutdown bool)
	Done(item interface{})
	ShutDown()
	ShutDownWithDrain()
	ShuttingDown() bool
}
```

实际使用队列时，用到的是Add、Get、Done方法，这里对这几个方法进行分析

Queue的结构体实现：

```go
type Type struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	queue []t

	// dirty defines all of the items that need to be processed.
	dirty set

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing set

	cond *sync.Cond
    // ......
}

```

- queue: queue定义了这个队列中的元素处理顺序，是一个[]interface{}类型的切片，可以保存任意类型的元素
- dirty: set的底层类型时map[interface{}]struct{}，queue中的元素集合会存到这个dirty set中，指待处理的items
- processing: 也是set类型，保存的是正在处理的items
- cond: 同步原语，用于在队列中有新元素时，通知调用Get进入wait状态的协程

整体的实现逻辑：当往队列中添加元素时，会添加到queue和dirty中，取出的正在处理的元素在processing中，元素处理完毕之后，会从processing中删除。为了确保队列中的元素不重复（避免多个协程同时处理同一个资源对象），所以引入了dirty，这样判断一个元素是不是已经存在的时间复杂度就是O(1)，而如果一个元素正在被处理，而同时被添加进来会只放入到dirty中，在处理完毕之后，才会从dirty中添加到queue中，也是为了避免并发处理同一个资源对象。

### 1.1 Queue的Add()方法

```go
func (q *Type) Add(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.has(item) {
		return
	}

	q.metrics.add(item)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	q.queue = append(q.queue, item)
	q.cond.Signal()
}
```

如果dirty中有这个元素就返回，否则就插入dirty中，而后就判断当前元素是否正在被处理，如果正在被处理就不放入queue中，避免被多个协程获取到并发处理。

### 1.2 Queue的Get()方法

```go
func (q *Type) Get() (item interface{}, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		// We must be shutting down.
		return nil, true
	}

	item = q.queue[0]
	// The underlying array still exists and reference this object, so the object will not be garbage collected.
	q.queue[0] = nil
	q.queue = q.queue[1:]

	q.metrics.get(item)

	q.processing.insert(item)
	q.dirty.delete(item)

	return item, false
}
```

`Get()`会尝试从queue中获取一个元素，同时将其加入到processing中，从dirty中删除，这里可以看到，如果之前queue中为空，导致获取元素的协程Wait的话，会被唤醒处理元素。

### 1.3 Queue.Done()方法

```go
func (q *Type) Done(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	q.processing.delete(item)
	if q.dirty.has(item) {
		q.queue = append(q.queue, item)
		q.cond.Signal()
	} else if q.processing.len() == 0 {
		q.cond.Signal()
	}
}
```

Done() 方法用来标记一个 item 被处理完成了。调用 Done() 方法的时候，这个 item 被从 processing set 中删除。另外前面提到 Add 的过程中如果发现元素在 `processing` 中存在，那么这个元素会被暂存到 `dirty ` 中。这里在处理元素的 Done 逻辑的时候也就顺带把暂存到 `dirty ` 中的元素取出来，加入到 queue 里去了。

## 2 DelayingQueue的实现

DelayingQueue的接口

```go
type DelayingInterface interface {
	Interface
	// AddAfter adds an item to the workqueue after the indicated duration has passed
	AddAfter(item interface{}, duration time.Duration)
}
```

这里的Interface{}就是前面的Queue的接口，在前面的基础上添加了一个AddAfter()方法，可以实现在一个item处理失败之后，在指定的延时之后再重新入队。

### 2.1 DelayingQueue.AddAfter()方法

```go
func (q *delayingType) AddAfter(item interface{}, duration time.Duration) {
	// don't add if we're already shutting down
	if q.ShuttingDown() {
		return
	}

	q.metrics.retry()

	// immediately add things with no delay
	if duration <= 0 {
		q.Add(item)
		return
	}

	select {
	case <-q.stopCh:
		// unblock if ShutDown() is called
	case q.waitingForAddCh <- &waitFor{data: item, readyAt: q.clock.Now().Add(duration)}:
	}
}
```

duration<=0时直接调用Add加入队列，当大于0时，会创建一个WaitFor对象放入到waitingForAddCh中，这个channel的消费可以查看DelayingType的创建函数：

```go
func newDelayingQueue(clock clock.WithTicker, q Interface, name string, provider MetricsProvider) *delayingType {
	ret := &delayingType{
		Interface:       q,
		clock:           clock,
		heartbeat:       clock.NewTicker(maxWait),
		stopCh:          make(chan struct{}),
		waitingForAddCh: make(chan *waitFor, 1000),
		metrics:         newRetryMetrics(name, provider),
	}

	go ret.waitingLoop()
	return ret
}

type waitFor struct {
	data    t
	readyAt time.Time
	// index in the priority queue (heap)
	index int
}
```

启动了一个协程去运行ret.waitingLoop()，创建的waitingForAddCh的容量大小是1000，内容就是waitFor

waitingLoop()方法：

```go
func (q *delayingType) waitingLoop() {
	defer utilruntime.HandleCrash()

	// Make a placeholder channel to use when there are no items in our list
	never := make(<-chan time.Time)

	// Make a timer that expires when the item at the head of the waiting queue is ready
	var nextReadyAtTimer clock.Timer

	// 这是一个优先级队列，用一个最小堆保存了所有 waitFor（waitForPriorityQueue 的类型是 []*waitFor）
	waitingForQueue := &waitForPriorityQueue{}
	heap.Init(waitingForQueue)

	waitingEntryByData := map[t]*waitFor{}

	// 后面的所有逻辑都在这个 for 循环里
	for {
		if q.Interface.ShuttingDown() {
			return
		}

		now := q.clock.Now()

		// Add ready entries
		// 一直判断 waitingForQueue 这个堆里有没有元素，如果有，就 Peek 出来第一个元素看是不是 ready，如果 ready
		// 那就通过 q.Add() 方法将其数据放入队列（也就是前文那个 Queue 实现的 Add 逻辑）
		for waitingForQueue.Len() > 0 {
			entry := waitingForQueue.Peek().(*waitFor)
			if entry.readyAt.After(now) {
				break
			}

			entry = heap.Pop(waitingForQueue).(*waitFor)
			q.Add(entry.data)
			delete(waitingEntryByData, entry.data)
		}

		// Set up a wait for the first item's readyAt (if one exists)
		// 代码到这里也就是说前面一个 for 里的 break 被执行了，换言之最小堆中的最小元素，也就是最快 ready 的一个都还没有 ready；
		// 这里将 nextReadyAtTimer 计时器的时间设置为（最快 ready 的第一个元素的 ready 时间 - 当前时间），
		// 这样当 nextReadyAt 这个 channel 有数据的时候，最小堆里的第一个元素也就 ready 了。
		nextReadyAt := never
		if waitingForQueue.Len() > 0 {
			if nextReadyAtTimer != nil {
				nextReadyAtTimer.Stop()
			}
			entry := waitingForQueue.Peek().(*waitFor)
			nextReadyAtTimer = q.clock.NewTimer(entry.readyAt.Sub(now))
			nextReadyAt = nextReadyAtTimer.C()
		}

		// 刚才设置好了一个合适的 nextReadyAt，现在开始 select 等待某个 channel 有反应
		select {
		case <-q.stopCh:
			return
		// 心跳时间是10s
		case <-q.heartbeat.C():
			// continue the loop, which will add ready items
		// 执行到这里也就是第一个 item ready 了
		case <-nextReadyAt:
			// continue the loop, which will add ready items

		// 当有元素被通过 AddAfter() 方法加进来时，waitingForAddCh 就会有内容，这时候会被取出来；
		// 如果没有 ready，那就调用 insert（大概率主要就是插入最小堆的过程）；如果 ready 了那就直接 Add 到 Queue 里。
		case waitEntry := <-q.waitingForAddCh:
			if waitEntry.readyAt.After(q.clock.Now()) {
				insert(waitingForQueue, waitingEntryByData, waitEntry)
			} else {
				q.Add(waitEntry.data)
			}
			// 通过一个循环将 waitingForAddCh 中的所有元素都消费掉，根据 ready 情况要么插入最小堆（优先级队列），要么直接入队。
			drained := false
			for !drained {
				select {
				case waitEntry := <-q.waitingForAddCh:
					if waitEntry.readyAt.After(q.clock.Now()) {
						insert(waitingForQueue, waitingEntryByData, waitEntry)
					} else {
						q.Add(waitEntry.data)
					}
				default:
					drained = true
				}
			}
		}
	}
}
```

insert()方法会判断在waitingForQueue这个优先级队列中是否存在对应元素，不存在就加入，存在就更新readyAt时间。

## 3 RateLimitingQueue

接口：

```go
// RateLimitingInterface is an interface that rate limits items being added to the queue.
type RateLimitingInterface interface {
	DelayingInterface

	// AddRateLimited adds an item to the workqueue after the rate limiter says it's ok
	AddRateLimited(item interface{})

	// Forget indicates that an item is finished being retried.  Doesn't matter whether it's for perm failing
	// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
	// still have to call `Done` on the queue.
	Forget(item interface{})

	// NumRequeues returns back how many times the item was requeued
	NumRequeues(item interface{}) int
}
```

这里关注AddRateLimited()方法和Forget()方法

```go
// AddRateLimited AddAfter's the item based on the time when the rate limiter says it's ok
func (q *rateLimitingType) AddRateLimited(item interface{}) {
	q.DelayingInterface.AddAfter(item, q.rateLimiter.When(item))
}

func (q *rateLimitingType) Forget(item interface{}) {
	q.rateLimiter.Forget(item)
}
```

通过ratelimiter确定item的ready时间，通过ratelimiter来Forget item

### 3.1 rateLimiter限速器的实现

接口：

```go
type RateLimiter interface {
	// When gets an item and gets to decide how long that item should wait
	When(item interface{}) time.Duration
	// Forget indicates that an item is finished being retried.  Doesn't matter whether it's for failing
	// or for success, we'll stop tracking it
	Forget(item interface{})
	// NumRequeues returns back how many failures the item has had
	NumRequeues(item interface{}) int
}
```

限速队列的实现：

```go
// 1 BucketRateLimiter
// 用了 golang 标准库的 golang.org/x/time/rate.Limiter 实现。BucketRateLimiter 实例化的时候比如传递一个 rate.NewLimiter(rate.Limit(10), 100) 进去，表示令牌桶里最多有 100 个令牌，每秒发放 10 个令牌。
type BucketRateLimiter struct {
   *rate.Limiter
}

var _ RateLimiter = &BucketRateLimiter{}

func (r *BucketRateLimiter) When(item interface{}) time.Duration {
   return r.Limiter.Reserve().Delay() // 过多久后给当前 item 发放一个令牌
}

func (r *BucketRateLimiter) NumRequeues(item interface{}) int {
   return 0
}

func (r *BucketRateLimiter) Forget(item interface{}) {
}

// 2 ItemExponentialFailureRateLimiter
// Exponential 是指数的意思，从这个限速器的名字大概能猜到是失败次数越多，限速越长而且是指数级增长的一种限速器。
type ItemExponentialFailureRateLimiter struct {
   failuresLock sync.Mutex
   failures     map[interface{}]int

   baseDelay time.Duration
   maxDelay  time.Duration
}

func (r *ItemExponentialFailureRateLimiter) When(item interface{}) time.Duration {
   r.failuresLock.Lock()
   defer r.failuresLock.Unlock()

   exp := r.failures[item]
   r.failures[item] = r.failures[item] + 1 // 失败次数加一

   // 每调用一次，exp 也就加了1，对应到这里时 2^n 指数爆炸
   backoff := float64(r.baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))
   if backoff > math.MaxInt64 { // 如果超过了最大整型，就返回最大延时，不然后面时间转换溢出了
      return r.maxDelay
   }

   calculated := time.Duration(backoff)
   if calculated > r.maxDelay { // 如果超过最大延时，则返回最大延时
      return r.maxDelay
   }

   return calculated
}

func (r *ItemExponentialFailureRateLimiter) NumRequeues(item interface{}) int {
   r.failuresLock.Lock()
   defer r.failuresLock.Unlock()

   return r.failures[item]
}

func (r *ItemExponentialFailureRateLimiter) Forget(item interface{}) {
   r.failuresLock.Lock()
   defer r.failuresLock.Unlock()

   delete(r.failures, item)
}

// 3 ItemFastSlowRateLimiter
// 快慢限速器，也就是先快后慢，定义一个阈值，超过了就慢慢重试。
type ItemFastSlowRateLimiter struct {
   failuresLock sync.Mutex
   failures     map[interface{}]int

   maxFastAttempts int            // 快速重试的次数
   fastDelay       time.Duration  // 快重试间隔
   slowDelay       time.Duration  // 慢重试间隔
}

func (r *ItemFastSlowRateLimiter) When(item interface{}) time.Duration {
   r.failuresLock.Lock()
   defer r.failuresLock.Unlock()

   r.failures[item] = r.failures[item] + 1 // 标识重试次数 + 1

   if r.failures[item] <= r.maxFastAttempts { // 如果快重试次数没有用完，则返回 fastDelay
      return r.fastDelay
   }

   return r.slowDelay // 反之返回 slowDelay
}

func (r *ItemFastSlowRateLimiter) NumRequeues(item interface{}) int {
   r.failuresLock.Lock()
   defer r.failuresLock.Unlock()

   return r.failures[item]
}

func (r *ItemFastSlowRateLimiter) Forget(item interface{}) {
   r.failuresLock.Lock()
   defer r.failuresLock.Unlock()

   delete(r.failures, item)
}

// 4 MaxOfRateLimiter
// 这个限速器内部放多个限速器，然后返回限速最长的一个延时
type MaxOfRateLimiter struct {
   limiters []RateLimiter
}

func (r *MaxOfRateLimiter) When(item interface{}) time.Duration {
   ret := time.Duration(0)
   for _, limiter := range r.limiters {
      curr := limiter.When(item)
      if curr > ret {
         ret = curr
      }
   }

   return ret
}

// 5 WithMaxWaitRateLimiter
// 在其他限速器上包装一个最大延迟的属性，如果到了最大延时，则直接返回
type WithMaxWaitRateLimiter struct {
   limiter  RateLimiter   // 其他限速器
   maxDelay time.Duration // 最大延时
}

func NewWithMaxWaitRateLimiter(limiter RateLimiter, maxDelay time.Duration) RateLimiter {
   return &WithMaxWaitRateLimiter{limiter: limiter, maxDelay: maxDelay}
}

func (w WithMaxWaitRateLimiter) When(item interface{}) time.Duration {
   delay := w.limiter.When(item)
   if delay > w.maxDelay {
      return w.maxDelay // 已经超过了最大延时，直接返回最大延时
   }

   return delay
}
```

自定义控制器的默认限速队列：

```go
func DefaultControllerRateLimiter() RateLimiter {
	return NewMaxOfRateLimiter(
		NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

func NewMaxOfRateLimiter(limiters ...RateLimiter) RateLimiter {
	return &MaxOfRateLimiter{limiters: limiters}
}
// 这里就是前面实现的令牌桶和指数延迟限速队列，会根据两个限速器返回的最大值作为限速结果。
```

## 总结

在编写自定义控制器的时候我们会用到这样的代码：

```
queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
```

这里实例化的队列是一个限速队列。

在 `ResourceEventHandlerFuncs` 里我们会写 `queue.Add(key)` 这样的代码，将一个 key（item/obj）加入到 workqueue 里。

控制器里还会写到这样的代码：

- `obj, shutdown := c.workqueue.Get()`
- `c.workqueue.Done(obj)`
- `c.workqueue.Forget(obj)`
- `c.workqueue.AddRateLimited(obj)`

这些方法也就对应着一个 obj 从 workqueue 中取出来（`Get()`），如果处理完成，就调用 `Done(obj)`，从而从队列中彻底移除，同时调用 `Forget(obj)` 告诉记速器可以忽略这个 obj 了（可别下次同名 obj 来的时候，误判了，让人家白等半天）。最后如果处理失败，遇到点啥临时异常情况，那就放回到 workqueue 里去，用 `AddRateLimited(obj)` 方法。