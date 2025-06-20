---
title: k8s源码分析（1）- ShareInformerFactory
summary: 
tags:
  - kubernetes
date: 2025-06-01
# external_link: http://github.com
---

最近准备对k8s源码进行梳理，希望能够更加深入地理解k8s的架构。接下来将整理一系列的博客，作为自己的笔记，也希望能帮助到他人。
<!--more-->

# ShareInformerFactory

## SharedInformerFactory的结构

使用 sharedInformerFactory 可以统一管理控制器中需要的各资源对象的 informer 实例，避免同一个资源创建多个实例。

```go
type sharedInformerFactory struct {
   client           kubernetes.Interface //clientset
   namespace        string //关注的namepace，可以通过WithNamespace Option配置
   tweakListOptions internalinterfaces.TweakListOptionsFunc
   lock             sync.Mutex
   defaultResync    time.Duration //前面传过来的时间，如30s
   customResync     map[reflect.Type]time.Duration //自定义resync时间
   informers map[reflect.Type]cache.SharedIndexInformer //针对每种类型资源存储一个informer，informer的类型是ShareIndexInformer
   startedInformers map[reflect.Type]bool //每个informer是否都启动了
}
```

- client: clientset，支持直接请求 api 中各内置资源对象的 restful group 客户端集合
- namespace: factory 关注的 namespace（默认 All Namespace），informer 中的 reflector 将只会 listAndWatch 指定 namespace 的资源
- defaultResync: 用于初始化持有的 shareIndexInformer 的 resyncCheckPeriod 和 defaultEventHandlerResyncPeriod 字段，用于定时的将 local store 同步到 deltaFIFO
- customResync：支持针对每一个 informer 来配置 resync 时间，通过 WithCustomResyncConfig 这个 Option 配置，否则就用指定的 defaultResync
- informers：factory 管理的 informer 集合
- startedInformers：记录已经启动的 informer 集合

## SharedInformerFactory对象的关键方法

### 创建sharedInformerFactory

```go
func NewSharedInformerFactoryWithOptions(client kubernetes.Interface, defaultResync time.Duration, options ...SharedInformerOption) SharedInformerFactory {
   factory := &sharedInformerFactory{
      client:           client,          //clientset，对原生资源来说，这里可以直接使用kube clientset
      namespace:        v1.NamespaceAll, //可以看到默认是监听所有ns下的指定资源
      defaultResync:    defaultResync,   //30s
      //以下初始化map结构
      informers:        make(map[reflect.Type]cache.SharedIndexInformer),
      startedInformers: make(map[reflect.Type]bool),
      customResync:     make(map[reflect.Type]time.Duration),
   }
   return factory
}
```

### 启动factory下的所有informer

```go
func (f *sharedInformerFactory) Start(stopCh <-chan struct{}) {
   f.lock.Lock()
   defer f.lock.Unlock()

   for informerType, informer := range f.informers {
      if !f.startedInformers[informerType] {
         //直接起gorouting调用informer的Run方法，并且标记对应的informer已经启动
         go informer.Run(stopCh)
         f.startedInformers[informerType] = true
      }
   }
}
```

### 等待informer的cache被同步

```go
func (f *sharedInformerFactory) WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool {
   //获取每一个已经启动的informer
   informers := func() map[reflect.Type]cache.SharedIndexInformer {
      f.lock.Lock()
      defer f.lock.Unlock()

      informers := map[reflect.Type]cache.SharedIndexInformer{}
      for informerType, informer := range f.informers {
         if f.startedInformers[informerType] {
            informers[informerType] = informer
         }
      }
      return informers
   }()

   res := map[reflect.Type]bool{}
   // 等待他们的cache被同步，调用的是informer的HasSynced方法
   for informType, informer := range informers {
      res[informType] = cache.WaitForCacheSync(stopCh, informer.HasSynced)
   }
   return res
}
```

等待每一个ShareIndexInformer的cache被同步，具体怎么算同步完成？

- sharedInformerFactory的WaitForCacheSync将会不断调用factory持有的所有informer的HasSynced方法，直到返回true
- 而informer的HasSynced方法调用的自己持有的controller的HasSynced方法（informer结构持有controller对象，下文会分析informer的结构）
- informer中的controller的HasSynced方法则调用的是controller持有的deltaFIFO对象的HasSynced方法

也就说sharedInformerFactory的WaitForCacheSync方法判断informer的cache是否同步，最终看的是informer中的deltaFIFO是否同步了，deltaFIFO的结构下文将会分析。

### Factory为自己添加informer

只有向factory中添加informer，factory才有意义，添加完成之后，上面factory的start方法就可以启动了

```go
// 向factory中注册指定的informer
func (f *sharedInformerFactory) InformerFor(obj runtime.Object, newFunc internalinterfaces.NewInformerFunc) cache.SharedIndexInformer {
   f.lock.Lock()
   defer f.lock.Unlock()
   //根据对象类型判断factory中是否已经有对应informer
   informerType := reflect.TypeOf(obj)
   informer, exists := f.informers[informerType]
   if exists {
      return informer
   }
   //如果factory中已经有这个对象类型的informer，就不创建了
   resyncPeriod, exists := f.customResync[informerType]
   if !exists {
      resyncPeriod = f.defaultResync
   }
   //没有就根据newFunc创建一个，并存在map中
   informer = newFunc(f.client, resyncPeriod)
   f.informers[informerType] = informer

   return informer
}
```

#### shareIndexInformer对应的newFunc的实现

client-go中已经为所有内置对象都提供了NewInformerFunc

以deployment为例，通过调用factory.Apps().V1().Deployments()即可为factory添加一个deployment对应的shareIndexInformer的实现，具体过程如下：

- 调用factory.Apps().V1().Deployments()即会调用以下Deployments方法创建deploymentInformer对象

```go
func (v *version) Deployments() DeploymentInformer {
    return &deploymentInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}
```

- 只要调用了factory.Apps().V1().Deployments()返回的deploymentInformer的Informer或Lister方法，就完成了向factory中添加deployment informer

```go
// deploymentInformer对象具有defaultInformer、Informer、Lister方法
// 可以看到创建deploymentInformer时传递了一个带索引的缓存，附带了一个namespace索引，后面可以了解带索引的缓存实现，比如可以支持查询：某个namespace下的所有pod

// 用于创建对应的shareIndexInformer，该方法提供给factory的InformerFor方法
func (f *deploymentInformer) defaultInformer(client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
    return NewFilteredDeploymentInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

// 向factor中添加dpeloyment的shareIndexInformer并返回
func (f *deploymentInformer) Informer() cache.SharedIndexInformer {
    return f.factory.InformerFor(&appsv1.Deployment{}, f.defaultInformer)
}

// 返回dpeloyment的lister对象，该lister中持有上面创建出的shareIndexInformer的cache的引用，方便通过缓存获取对象
func (f *deploymentInformer) Lister() v1.DeploymentLister {
    return v1.NewDeploymentLister(f.Informer().GetIndexer())
}
```

- deploymentInformer的defaultInformer方法将会创建出一个shareIndexInformer

```go
// 可先看看下面的shareIndexInformer结构
func NewFilteredDeploymentInformer(client kubernetes.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
   return cache.NewSharedIndexInformer(
      // 定义对象的ListWatch方法，这里直接用的是clientset中的方法
      &cache.ListWatch{
         ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
            if tweakListOptions != nil {
               tweakListOptions(&options)
            }
            return client.AppsV1beta1().Deployments(namespace).List(options)
         },
         WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
            if tweakListOptions != nil {
               tweakListOptions(&options)
            }
            return client.AppsV1beta1().Deployments(namespace).Watch(options)
         },
      },
      &appsv1beta1.Deployment{},
      resyncPeriod, //创建factory是指定的时间，如30s
      indexers,
   )
}
```

## ShareIndexInformer结构

indexer：底层缓存，其实就是一个map记录对象，再通过一些其他map在插入删除对象是根据索引函数维护索引key如ns与对象pod的关系 

controller：informer内部的一个controller，这个controller包含reflector：根据用户定义的ListWatch方法获取对象并更新增量队列DeltaFIFO 

processor：知道如何处理DeltaFIFO队列中的对象，实现是sharedProcessor{} 

listerWatcher：知道如何list对象和watch对象的方法 

objectType：deployment{} 

resyncCheckPeriod: 给自己的controller的reflector每隔多少s<尝试>调用listener的shouldResync方法 defaultEventHandlerResyncPeriod：通过AddEventHandler方法给informer配置回调时如果没有配置的默认值，这个值用在processor的listener中判断是否需要进行resync，最小1s

两个字段的默认值都是来自创建factory时指定的defaultResync，当resyncPeriod < s.resyncCheckPeriod时，如果informer已经启动了才添加的EventHandler，那么调整resyncPeriod为resyncCheckPeriod，否则调整resyncCheckPeriod为resyncPeriod

```go
type sharedIndexInformer struct {
   indexer    Indexer //informer中的底层缓存cache
   controller Controller //持有reflector和deltaFIFO对象，reflector对象将会listWatch对象添加到deltaFIFO，同时更新indexer cahce，更新成功则通过sharedProcessor触发用户配置的Eventhandler

   processor             *sharedProcessor //持有一系列的listener，每个listener对应用户的EventHandler
   cacheMutationDetector MutationDetector //可以先忽略，这个对象可以用来监测local cache是否被外部直接修改

   // This block is tracked to handle late initialization of the controller
   listerWatcher ListerWatcher //deployment的listWatch方法
   objectType    runtime.Object

   // resyncCheckPeriod is how often we want the reflector's resync timer to fire so it can call
   // shouldResync to check if any of our listeners need a resync.
   resyncCheckPeriod time.Duration
   // defaultEventHandlerResyncPeriod is the default resync period for any handlers added via
   // AddEventHandler (i.e. they don't specify one and just want to use the shared informer's default
   // value).
   defaultEventHandlerResyncPeriod time.Duration
   // clock allows for testability
   clock clock.Clock

   started, stopped bool
   startedLock      sync.Mutex

   // blockDeltas gives a way to stop all event distribution so that a late event handler
   // can safely join the shared informer.
   blockDeltas sync.Mutex
}
```

### sharedIndexInformer的Run()方法：

```go
func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	if s.HasStarted() {
		klog.Warningf("The sharedIndexInformer has started, run more than once is not allowed")
		return
	}

	func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()

		fifo := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
			KnownObjects:          s.indexer,
			EmitDeltaTypeReplaced: true,
			Transformer:           s.transform,
		})

		cfg := &Config{
			Queue:             fifo,
			ListerWatcher:     s.listerWatcher,
			ObjectType:        s.objectType,
			ObjectDescription: s.objectDescription,
			FullResyncPeriod:  s.resyncCheckPeriod,
			RetryOnError:      false,
			ShouldResync:      s.processor.shouldResync,

			Process:           s.HandleDeltas,
			WatchErrorHandler: s.watchErrorHandler,
		}

		s.controller = New(cfg)
		s.controller.(*controller).clock = s.clock
		s.started = true
	}()

	// Separate stop channel because Processor should be stopped strictly after controller
	processorStopCh := make(chan struct{})
	var wg wait.Group
	defer wg.Wait()              // Wait for Processor to stop
	defer close(processorStopCh) // Tell Processor to stop
	wg.StartWithChannel(processorStopCh, s.cacheMutationDetector.Run)
	wg.StartWithChannel(processorStopCh, s.processor.run)

	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true // Don't want any new listeners
	}()
	s.controller.Run(stopCh)
}
```

 可以看到会先创建DeltaFIFO对象，这个对象内部持有informer的Indexer，后续在进行初始化本地资源时会用到，然后创建informer的controller的对象，启动processor，processor的listener就是根据用户自定义创建的ResourceEventHandler的封装，用于处理deltas对象（DeltaFIFO的元素），然后启动controller。

### s.controller.Run():

```go
func (c *controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	go func() {
		<-stopCh
		c.config.Queue.Close()
	}()
	r := NewReflectorWithOptions(
		c.config.ListerWatcher,
		c.config.ObjectType,
		c.config.Queue,
		ReflectorOptions{
			ResyncPeriod:    c.config.FullResyncPeriod,
			TypeDescription: c.config.ObjectDescription,
			Clock:           c.clock,
		},
	)
	r.ShouldResync = c.config.ShouldResync
	r.WatchListPageSize = c.config.WatchListPageSize
	if c.config.WatchErrorHandler != nil {
		r.watchErrorHandler = c.config.WatchErrorHandler
	}

	c.reflectorMutex.Lock()
	c.reflector = r
	c.reflectorMutex.Unlock()

	var wg wait.Group

	wg.StartWithChannel(stopCh, r.Run)

	wait.Until(c.processLoop, time.Second, stopCh)
	wg.Wait()
}

func NewReflectorWithOptions(lw ListerWatcher, expectedType interface{}, store Store, options ReflectorOptions) *Reflector {
	reflectorClock := options.Clock
	if reflectorClock == nil {
		reflectorClock = clock.RealClock{}
	}
	r := &Reflector{
		name:            options.Name,
		resyncPeriod:    options.ResyncPeriod,
		typeDescription: options.TypeDescription,
		listerWatcher:   lw,
		store:           store,
		// We used to make the call every 1sec (1 QPS), the goal here is to achieve ~98% traffic reduction when
		// API server is not healthy. With these parameters, backoff will stop at [30,60) sec interval which is
		// 0.22 QPS. If we don't backoff for 2min, assume API server is healthy and we reset the backoff.
		backoffManager:    wait.NewExponentialBackoffManager(800*time.Millisecond, 30*time.Second, 2*time.Minute, 2.0, 1.0, reflectorClock),
		clock:             reflectorClock,
		watchErrorHandler: WatchErrorHandler(DefaultWatchErrorHandler),
		expectedType:      reflect.TypeOf(expectedType),
	}
	......
	return r
}

func (r *Reflector) Run(stopCh <-chan struct{}) {
	klog.V(3).Infof("Starting reflector %s (%s) from %s", r.typeDescription, r.resyncPeriod, r.name)
	wait.BackoffUntil(func() {
		if err := r.ListAndWatch(stopCh); err != nil {
			r.watchErrorHandler(r, err)
		}
	}, r.backoffManager, true, stopCh)
	klog.V(3).Infof("Stopping reflector %s (%s) from %s", r.typeDescription, r.resyncPeriod, r.name)
}
```

controller会创建Reflector对象，持有DeltaFiFo对象，然后调用reflector的run方法list&watch apiserver，list&watch的过程后面会进行分析。自此Informer启动流程完成。

### sharedIndexInformer.HasSynced()

前面讲到Factory会等待持有的Informer Sync完成，整个调用流程：sharedIndexInformer.HasSynced->s.controller.HasSynced()->c.config.Queue.HasSynced()，最终判断的是DeltaFIFO是否同步完成，这里对此过程进行分析。

```go
func (s *sharedIndexInformer) HasSynced() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return false
	}
	return s.controller.HasSynced()
}

func (c *controller) HasSynced() bool {
	return c.config.Queue.HasSynced()
}

func (f *DeltaFIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.hasSynced_locked()
}

func (f *DeltaFIFO) hasSynced_locked() bool {
	return f.populated && f.initialPopulationCount == 0
}
```

### DeltaFIFO的结构体

```go
type DeltaFIFO struct {
	// lock/cond protects access to 'items' and 'queue'.
	lock sync.RWMutex
	cond sync.Cond

	// `items` maps a key to a Deltas.
	// Each such Deltas has at least one Delta.
	items map[string]Deltas

	// `queue` maintains FIFO order of keys for consumption in Pop().
	// There are no duplicates in `queue`.
	// A key is in `queue` if and only if it is in `items`.
	queue []string

	// populated is true if the first batch of items inserted by Replace() has been populated
	// or Delete/Add/Update/AddIfNotPresent was called first.
	populated bool
	// initialPopulationCount is the number of items inserted by the first call of Replace()
	initialPopulationCount int

	// keyFunc is used to make the key used for queued item
	// insertion and retrieval, and should be deterministic.
	keyFunc KeyFunc

	// knownObjects list keys that are "known" --- affecting Delete(),
	// Replace(), and Resync()
	knownObjects KeyListerGetter

	// Used to indicate a queue is closed so a control loop can exit when a queue is empty.
	// Currently, not used to gate any of CRUD operations.
	closed bool

	// emitDeltaTypeReplaced is whether to emit the Replaced or Sync
	// DeltaType when Replace() is called (to preserve backwards compat).
	emitDeltaTypeReplaced bool

	// Called with every object if non-nil.
	transformer TransformFunc
}
```

populated字段只要对DeltaFIFO进行操作（初始化Replace()、add/update等）就会被赋值为true，而initialPopulationCount是初次被Replace()时所插入的items数量。

```go
func (f *DeltaFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
			// When Close() is called, the f.closed is set and the condition is broadcasted.
			// Which causes this loop to continue and return from the Pop().
			if f.closed {
				return nil, ErrFIFOClosed
			}

			f.cond.Wait()
		}
		isInInitialList := !f.hasSynced_locked()
		id := f.queue[0]
		f.queue = f.queue[1:]
		depth := len(f.queue)
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
		}
		item, ok := f.items[id]
		if !ok {
			// This should never happen
			klog.Errorf("Inconceivable! %q was in f.queue but not f.items; ignoring.", id)
			continue
		}
		delete(f.items, id)
		// Only log traces if the queue depth is greater than 10 and it takes more than
		// 100 milliseconds to process one item from the queue.
		// Queue depth never goes high because processing an item is locking the queue,
		// and new items can't be added until processing finish.
		// https://github.com/kubernetes/kubernetes/issues/103789
		if depth > 10 {
			trace := utiltrace.New("DeltaFIFO Pop Process",
				utiltrace.Field{Key: "ID", Value: id},
				utiltrace.Field{Key: "Depth", Value: depth},
				utiltrace.Field{Key: "Reason", Value: "slow event handlers blocking the queue"})
			defer trace.LogIfLong(100 * time.Millisecond)
		}
		err := process(item, isInInitialList)
		if e, ok := err.(ErrRequeue); ok {
			f.addIfNotPresent(id, item)
			err = e.Err
		}
		// Don't need to copyDeltas here, because we're transferring
		// ownership to the caller.
		return item, err
	}
}
```

DeltaFIFO.Pop()方法会取DeltaFIFO中的item并进行处理，往前追溯：

```go
func (c *controller) processLoop() {
	for {
		obj, err := c.config.Queue.Pop(PopProcessFunc(c.config.Process))
		if err != nil {
			if err == ErrFIFOClosed {
				return
			}
			if c.config.RetryOnError {
				// This is the safe way to re-enqueue.
				c.config.Queue.AddIfNotPresent(obj)
			}
		}
	}
}

// sharedIndexInformer.Run()方法内创建的config,
cfg := &Config{
    Queue:             fifo,
    ListerWatcher:     s.listerWatcher,
    ObjectType:        s.objectType,
    ObjectDescription: s.objectDescription,
    FullResyncPeriod:  s.resyncCheckPeriod,
    RetryOnError:      false,
    ShouldResync:      s.processor.shouldResync,

    Process:           s.HandleDeltas,
    WatchErrorHandler: s.watchErrorHandler,
}

func (s *sharedIndexInformer) HandleDeltas(obj interface{}, isInInitialList bool) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	if deltas, ok := obj.(Deltas); ok {
		return processDeltas(s, s.indexer, deltas, isInInitialList)
	}
	return errors.New("object given as Process argument is not Deltas")
}

func processDeltas(
	// Object which receives event notifications from the given deltas
	handler ResourceEventHandler,
	clientState Store,
	deltas Deltas,
	isInInitialList bool,
) error {
	// from oldest to newest
	for _, d := range deltas {
		obj := d.Object

		switch d.Type {
		case Sync, Replaced, Added, Updated:
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				if err := clientState.Update(obj); err != nil {
					return err
				}
				handler.OnUpdate(old, obj)
			} else {
				if err := clientState.Add(obj); err != nil {
					return err
				}
				handler.OnAdd(obj, isInInitialList)
			}
		case Deleted:
			if err := clientState.Delete(obj); err != nil {
				return err
			}
			handler.OnDelete(obj)
		}
	}
	return nil
}
```

可以看到DeltaFIFO.Pop()方法最终是调用了sharedIndexInformer的HandlerDeltas，delta包含了对象以及对象的操作类型，后面根据delta的类型调用的indexer执行更新、添加删除操作，初始化时都是add操作，等待initialPopulationCount为0就说明把从apiserver list的全部内容给同步到本地的indexer中了。