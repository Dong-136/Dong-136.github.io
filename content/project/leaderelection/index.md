---
title: client-go的leaderelection机制
summary: leaderelection的选择机制
tags:
  - kubernetes
date: 2025-06-01
toc: true
# external_link: http://github.com
---

# 1. 背景

kubernetes控制面的组件需要保证高可用机制，来确保整个集群业务的高可用性，目前的做法为部署多个实例，通过领导人选举的方式来确保集群业务的高可用性。

# 2.实现机制

对于控制面组件的各个实例，同一时间内只能有一个有状态的实例提供服务，比如：kube-controller-manager、scheduler。leaderelection机制是实现了一个lease，各个实例去竞争这个lease，获取到lease的实例成为leader并维护租约，其余实例维护循环来定期检查租约是否过期，在leader崩溃时，其他正常实例获取lease提供服务。ps：这里的实现机制依赖kuberentes的乐观锁，lease也是k8s的资源，同一时间内只能有一个修改成功；因为不涉及成员之间的配合、通信，所以实现起来比较简单，也不需要满足类似etcd的仲裁原则，只要有一个实例是存活的，该组件就能正常工作。

# 3.源码分析

代码路径在client-go/tools/leaderelection

![](/Users/dong/Desktop/image2025-1-16_19-23-46.png)

## Interface接口

`Interface`中定义了一系列方法, 包括增加、修改、获取一个`LeaderElectionRecord`, 说白了就是一个客户端, 而且每个客户端实例都要有自己分布式唯一的id。

```go
// tools/leaderelection/resourcelock/interface.go
  
// 资源占有者的描述信息
type LeaderElectionRecord struct {
    // 持有锁进程的标识 也就是leader的id
    HolderIdentity       string      `json:"holderIdentity"`
    // 一个租约多长时间
    LeaseDurationSeconds int         `json:"leaseDurationSeconds"`
    // 获得leader的时间
    AcquireTime          metav1.Time `json:"acquireTime"`
    // 续约的时间
    RenewTime            metav1.Time `json:"renewTime"`
    // leader变更的次数
    LeaderTransitions    int         `json:"leaderTransitions"`
}
  
type Interface interface {
    // 返回当前资源LeaderElectionRecord
    Get() (*LeaderElectionRecord, error)
    // 创建一个资源LeaderElectionRecord
    Create(ler LeaderElectionRecord) error
    // 更新资源
    Update(ler LeaderElectionRecord) error
    // 记录事件
    RecordEvent(string)
    // 返回当前该应用的id
    Identity() string
    // 描述信息（namespace/name）
    Describe() string
}
```

Interface有四个实现类, 分别为`EndpointLock`, `ConfigMapLock`、`LeaseLock`和`MultiLock`（一般不用），分别可以操作Kubernetes中的endpoint, configmap和lease。这里以`LeaseLock`为例子说明。

```go
// tools/leaderelection/resourcelock/leaselock.go
 
type LeaseLock struct {
    // LeaseMeta should contain a Name and a Namespace of a
    // LeaseMeta object that the LeaderElector will attempt to lead.
    LeaseMeta  metav1.ObjectMeta
    // 访问api-server的客户端
    Client     coordinationv1client.LeasesGetter
    // 该LeaseLock的分布式唯一身份id
    LockConfig ResourceLockConfig
    // 资源锁对应的lease资源对象
    lease      *coordinationv1.Lease
}
 
// tools/leaderelection/resourcelock/interface.go
type ResourceLockConfig struct {
    // 分布式唯一id
    Identity string
    EventRecorder EventRecorder
}
```

LeaseLock类型对应函数详解：`Create`, `Update`, `Get`方法都是利用`client`去访问kubernetes的apiserver。

```go
// tools/leaderelection/resourcelock/leaselock.go
 
// 通过访问apiserver获取当前资源锁对象ll.lease，并组织返回对应的LeaderElectionRecord对象和LeaderElectionRecord序列化值
// Get returns the election record from a Lease spec
func (ll *LeaseLock) Get(ctx context.Context) (*LeaderElectionRecord, []byte, error) {
    var err error
    // 获取资源锁对应的资源对象ll.lease
    ll.lease, err = ll.Client.Leases(ll.LeaseMeta.Namespace).Get(ctx, ll.LeaseMeta.Name, metav1.GetOptions{})
    if err != nil {
        return nil, nil, err
    }
    // 利用lease资源对象spec生成对应LeaderElectionRecord资源对象
    record := LeaseSpecToLeaderElectionRecord(&ll.lease.Spec)
    // 序列化LeaderElectionRecord资源对象（byte[]）
    recordByte, err := json.Marshal(*record)
    if err != nil {
        return nil, nil, err
    }
    return record, recordByte, nil
}
 
// 根据LeaderElectionRecord创建对应资源锁对象 ll.lease
// Create attempts to create a Lease
func (ll *LeaseLock) Create(ctx context.Context, ler LeaderElectionRecord) error {
    var err error
    ll.lease, err = ll.Client.Leases(ll.LeaseMeta.Namespace).Create(ctx, &coordinationv1.Lease{
        ObjectMeta: metav1.ObjectMeta{
            Name:      ll.LeaseMeta.Name,
            Namespace: ll.LeaseMeta.Namespace,
        },
        // 利用ElectionRecord资源对象生成对应lease资源对象spec
        Spec: LeaderElectionRecordToLeaseSpec(&ler),
    }, metav1.CreateOptions{})
    return err
}
 
// Update will update an existing Lease spec.
func (ll *LeaseLock) Update(ctx context.Context, ler LeaderElectionRecord) error {
    if ll.lease == nil {
        return errors.New("lease not initialized, call get or create first")
    }
    // 利用ElectionRecord资源对象生成对应lease资源对象spec
    ll.lease.Spec = LeaderElectionRecordToLeaseSpec(&ler)
 
    lease, err := ll.Client.Leases(ll.LeaseMeta.Namespace).Update(ctx, ll.lease, metav1.UpdateOptions{})
    if err != nil {
        return err
    }
 
    ll.lease = lease
    return nil
}
 
// RecordEvent in leader election while adding meta-data
func (ll *LeaseLock) RecordEvent(s string) {
    if ll.LockConfig.EventRecorder == nil {
        return
    }
    events := fmt.Sprintf("%v %v", ll.LockConfig.Identity, s)
    ll.LockConfig.EventRecorder.Eventf(&coordinationv1.Lease{ObjectMeta: ll.lease.ObjectMeta}, corev1.EventTypeNormal, "LeaderElection", events)
}
 
// Describe is used to convert details on current resource lock
// into a string
func (ll *LeaseLock) Describe() string {
    return fmt.Sprintf("%v/%v", ll.LeaseMeta.Namespace, ll.LeaseMeta.Name)
}
 
// Identity returns the Identity of the lock
func (ll *LeaseLock) Identity() string {
    return ll.LockConfig.Identity
}
 
 
// 利用lease资源对象spec生成对应LeaderElectionRecord资源对象
func LeaseSpecToLeaderElectionRecord(spec *coordinationv1.LeaseSpec) *LeaderElectionRecord {
    var r LeaderElectionRecord
    if spec.HolderIdentity != nil {
        r.HolderIdentity = *spec.HolderIdentity
    }
    if spec.LeaseDurationSeconds != nil {
        r.LeaseDurationSeconds = int(*spec.LeaseDurationSeconds)
    }
    if spec.LeaseTransitions != nil {
        r.LeaderTransitions = int(*spec.LeaseTransitions)
    }
    if spec.AcquireTime != nil {
        r.AcquireTime = metav1.Time{spec.AcquireTime.Time}
    }
    if spec.RenewTime != nil {
        r.RenewTime = metav1.Time{spec.RenewTime.Time}
    }
    return &r
 
}
 
// 利用ElectionRecord资源对象生成对应lease资源对象spec
func LeaderElectionRecordToLeaseSpec(ler *LeaderElectionRecord) coordinationv1.LeaseSpec {
    leaseDurationSeconds := int32(ler.LeaseDurationSeconds)
    leaseTransitions := int32(ler.LeaderTransitions)
    return coordinationv1.LeaseSpec{
        HolderIdentity:       &ler.HolderIdentity,
        LeaseDurationSeconds: &leaseDurationSeconds,
        AcquireTime:          &metav1.MicroTime{ler.AcquireTime.Time},
        RenewTime:            &metav1.MicroTime{ler.RenewTime.Time},
        LeaseTransitions:     &leaseTransitions,
    }
}
```

## LeaderElector

### LeaderElectionConfig:

定义了一些竞争资源的参数，用于保存当前应用的一些配置，包括资源锁、持有锁的时间等，LeaderElectionConfig.lock 支持保存在以下三种资源中：

- `configmap`
- `endpoint`
- `lease`

包中还提供了一个 `multilock` ，即可以进行选择两种，当其中一种保存失败时，选择第二种。

```go
//client-go/tools/leaderelection/leaderelection.go
  
type LeaderElectionConfig struct {
    // Lock 的类型
    Lock rl.Interface
    //持有锁的时间
    LeaseDuration time.Duration
    //在更新租约的超时时间
    RenewDeadline time.Duration
    //竞争获取锁的时间
    RetryPeriod time.Duration
    //需要用户配置的状态变化时执行的函数，支持三种：
    //1、OnStartedLeading 启动是执行的业务代码
    //2、OnStoppedLeading leader停止执行的方法
    //3、OnNewLeader 当产生新的leader后执行的方法
    Callbacks LeaderCallbacks
  
    //进行监控检查
    // WatchDog is the associated health checker
    // WatchDog may be null if its not needed/configured.
    WatchDog *HealthzAdaptor
    //leader退出时，是否执行release方法
    ReleaseOnCancel bool
  
    // Name is the name of the resource lock for debugging
    Name string
}
```

### LeaderElector:

进行资源竞争的实体

```go
//client-go/tools/leaderelection/leaderelection.go// LeaderElector is a leader election client.
type LeaderElector struct {
    // 用于保存当前应用的一些配置
    config LeaderElectionConfig
    // 通过apiserver远程获取的资源锁对象 (不一定自己是leader) 所有想竞争此资源的应用获取的是同一份
    // internal bookkeeping
    observedRecord    rl.LeaderElectionRecord
    //资源锁对象spec，用于和远程获取的资源锁对象值比较
    observedRawRecord []byte
    // 获取的时间
    observedTime      time.Time
    // used to implement OnNewLeader(), may lag slightly from the
    // value observedRecord.HolderIdentity if the transition has
    // not yet been reported.
    reportedLeader string
  
    // clock is wrapper around time to allow for less flaky testing
    clock clock.Clock
  
    metrics leaderMetricsAdapter
}
```

这里着重要关注以下几个属性:

config: 该LeaderElectionConfig对象配置了当前应用的客户端, 以及此客户端的唯一id等等。
observedRecord: 该LeaderElectionRecord就是保存着从api-server中获得的leader的信息。
observedTime: 获得的时间。

很明显判断当前进程是不是leader只需要判断config中的id和observedRecord中的id是不是一致即可.

```go
func (le *LeaderElector) GetLeader() string {
    return le.observedRecord.HolderIdentity
}
   
// IsLeader returns true if the last observed leader was this client else returns false.
func (le *LeaderElector) IsLeader() bool {
    return le.observedRecord.HolderIdentity == le.config.Lock.Identity()
}
```

### leaderElection运行逻辑

#### run

```go
func (le *LeaderElector) Run(ctx context.Context) {
    defer func() {
        runtime.HandleCrash()
        le.config.Callbacks.OnStoppedLeading()
    }()
    // 如果获取成功 那就是ctx signalled done
    // 不然即使失败, 该client也会一直去尝试获得leader位置
    if !le.acquire(ctx) {
        return // ctx signalled done
    }
    // 如果获得leadership 以goroutine和回调的形式启动用户自己的逻辑方法OnStartedLeading
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()
    go le.config.Callbacks.OnStartedLeading(ctx)
    // 一直去续约 这里也是一个循环操作
    // 如果失去了leadership 该方法才会返回
    // 该方法返回 整个Run方法就返回了
    le.renew(ctx)
}　
```

1. 该client(也就是le这个实例)首先会调用acquire方法一直尝试去竞争leadership（如果竞争失败, 继续竞争, 不会进入2. 竞争成功, 进入2）。
2. 异步启动用户自己的逻辑程序(OnStartedLeading)（进入3）。
3. 通过调用renew方法续约自己的leadership. 续约成功, 继续续约，续约失败, 整个Run就结束了。

#### acquire()

```go
//检查是否需要广播新产生的leader
func (le *LeaderElector) maybeReportTransition() {
    // 如果没有变化 则不需要更新
    if le.observedRecord.HolderIdentity == le.reportedLeader {
        return
    }
    // 更新reportedLeader为最新的leader的id
    le.reportedLeader = le.observedRecord.HolderIdentity
    if le.config.Callbacks.OnNewLeader != nil {
        // 调用当前应用的回调函数OnNewLeader报告新的leader产生
        go le.config.Callbacks.OnNewLeader(le.reportedLeader)
    }
}
   
// 一旦获得leadership 立马返回true，那就是ctx signalled done
// 失败的话，该client会一直去尝试获得leader位置
func (le *LeaderElector) acquire(ctx context.Context) bool {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()
    succeeded := false
    desc := le.config.Lock.Describe()
    klog.Infof("attempting to acquire leader lease  %v...", desc)
    wait.JitterUntil(func() {
        // 尝试获得或者更新资源
        succeeded = le.tryAcquireOrRenew()
        // 有可能会产生新的leader
        // 所以调用maybeReportTransition检查是否需要广播新产生的leader
        le.maybeReportTransition()
        if !succeeded {
            // 如果获得leadership失败 则返回后继续竞争
            klog.V(4).Infof("failed to acquire lease %v", desc)
            return
        }
        // 自己成为leader
        // 可以调用cancel方法退出JitterUntil进而从acquire中返回
        le.config.Lock.RecordEvent("became leader")
        le.metrics.leaderOn(le.config.Name)
        klog.Infof("successfully acquired lease %v", desc)
        cancel()
    }, le.config.RetryPeriod, JitterFactor, true, ctx.Done())
    return succeeded
}
```

acquire的作用如下:

一旦获得`leadership，`立马返回`true，`否则会隔`RetryPeriod`时间尝试一次。这里的逻辑比较简单, 主要的逻辑是在`tryAcquireOrRenew`方法中。

#### Renew and release

```go
// RenewDeadline=15s RetryPeriod=5s
// renew loops calling tryAcquireOrRenew and returns immediately when tryAcquireOrRenew fails or ctx signals done.
func (le *LeaderElector) renew(ctx context.Context) {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()
    // 每隔RetryPeriod会调用 除非cancel()方法被调用才会退出
    wait.Until(func() {
        timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
        defer timeoutCancel()
        // 每隔5s调用该方法直到该方法返回true为止
        // 如果超时了也会退出该方法 并且err中有错误信息
        err := wait.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
            return le.tryAcquireOrRenew(timeoutCtx), nil
        }, timeoutCtx.Done())
  
        // 有可能会产生新的leader 如果有会广播新产生的leader
        le.maybeReportTransition()
        desc := le.config.Lock.Describe()
        if err == nil {
            // 如果err == nil, 表明上面PollImmediateUntil中返回true了 续约成功 依然处于leader位置
            // 返回后 继续运行wait.Until的逻辑
            klog.V(4).Infof("successfully renewed lease %v", desc)
            return
        }
        // err != nil 表明超时了 试的总时间超过了RenewDeadline 失去了leader位置 续约失败
        // 调用cancel方法退出wait.Until
        le.config.Lock.RecordEvent("stopped leading")
        le.metrics.leaderOff(le.config.Name)
        klog.Infof("failed to renew lease %v: %v", desc, err)
        cancel()
    }, le.config.RetryPeriod, ctx.Done())
  
    // if we hold the lease, give it up
    if le.config.ReleaseOnCancel {
        le.release()
    }
}
  
// leader续约cancel()的时候释放资源锁对象holderIdentity字段的值
// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release() bool {
    if !le.IsLeader() {
        return true
    }
    now := metav1.Now()
    leaderElectionRecord := rl.LeaderElectionRecord{
        LeaderTransitions:    le.observedRecord.LeaderTransitions,
        LeaseDurationSeconds: 1,
        RenewTime:            now,
        AcquireTime:          now,
    }
    if err := le.config.Lock.Update(context.TODO(), leaderElectionRecord); err != nil {
        klog.Errorf("Failed to release lock: %v", err)
        return false
    }
    le.observedRecord = leaderElectionRecord
    le.observedTime = le.clock.Now()
    return true
}
```

可以看到该client的base条件是它自己是当前的leader, 然后来续约操作。

这里来说一下RenewDeadline和RetryPeriod的作用。
每隔RetryPeriod时间会通过tryAcquireOrRenew续约, 如果续约失败, 还会进行再次尝试. 一直到尝试的总时间超过RenewDeadline后该client就会失去leadership。

#### tryAcquireOrRenew

```go
// 竞争或者更新leadership
// 成功返回true 失败返回false
func (le *LeaderElector) tryAcquireOrRenew(ctx context.Context) bool {
    now := metav1.Now()
    leaderElectionRecord := rl.LeaderElectionRecord{
        HolderIdentity:       le.config.Lock.Identity(),
        LeaseDurationSeconds: int(le.config.LeaseDuration / time.Second),
        RenewTime:            now,
        AcquireTime:          now,
    }
  
    // 1. obtain or create the ElectionRecord
    // client通过apiserver获得ElectionRecord和ElectionRecord序列化值
    oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := le.config.Lock.Get(ctx)
    if err != nil {
        if !errors.IsNotFound(err) {
            // 失败直接退出
            klog.Errorf("error retrieving resource lock %v: %v", le.config.Lock.Describe(), err)
            return false
        }
        // 因为没有获取到, 因此创建一个新的进去
        if err = le.config.Lock.Create(ctx, leaderElectionRecord); err != nil {
            klog.Errorf("error initially creating leader election record: %v", err)
            return false
        }
        // 然后设置observedRecord为刚刚加入进去的leaderElectionRecord
        le.observedRecord = leaderElectionRecord
        le.observedTime = le.clock.Now()
        return true
    }
  
    // 2. Record obtained, check the Identity & Time
    // 从远端获取到record(资源)成功存到oldLeaderElectionRecord
    // 如果oldLeaderElectionRecord与observedRecord不相同 更新observedRecord
    // 因为observedRecord代表是从远端存在Record
  
    // 需要注意的是每个client都在竞争leadership, 而leader一直在续约, leader会更新它的RenewTime字段
    // 所以一旦leader续约成功 每个non-leader候选者都需要更新其observedTime和observedRecord
    if !bytes.Equal(le.observedRawRecord, oldLeaderElectionRawRecord) {
        le.observedRecord = *oldLeaderElectionRecord
        le.observedRawRecord = oldLeaderElectionRawRecord
        le.observedTime = le.clock.Now()
    }
    // 如果leader已经被占有并且不是当前自己这个应用, 而且时间还没有到期
    // 那就直接返回false, 因为已经无法抢占 时间没有过期
    if len(oldLeaderElectionRecord.HolderIdentity) > 0 &&
        le.observedTime.Add(le.config.LeaseDuration).After(now.Time) &&
        !le.IsLeader() {
        klog.V(4).Infof("lock is held by %v and has not yet expired", oldLeaderElectionRecord.HolderIdentity)
        return false
    }
  
    // 3. We're going to try to update. The leaderElectionRecord is set to it's default
    // here. Let's correct it before updating.
    if le.IsLeader() {
        // 如果当前服务就是以前的占有者
        leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
        leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
    } else {
        // 如果当前服务不是以前的占有者 LeaderTransitions加1
        leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
    }
  
    // update the lock itself
    // 当前client占有该资源 成为leader
    if err = le.config.Lock.Update(ctx, leaderElectionRecord); err != nil {
        klog.Errorf("Failed to update lock: %v", err)
        return false
    }
  
    le.observedRecord = leaderElectionRecord
    le.observedTime = le.clock.Now()
    return true
}

```

这里需要注意的是当前client不是leader的时候, 如何去判断一个leader是否已经expired了?

   通过le.observedTime.Add(le.config.LeaseDuration).After(now.Time)；

- le.observedTime: 代表的是获得leader(截止当前时间为止的最后一次renew)对象的时间；
- le.config.LeaseDuration: 当前进程获得leadership需要的等待时间；
- le.observedTime.Add(le.config.LeaseDuration): 就是自己(当前进程)被允许获得leadership的时间。

如果le.observedTime.Add(le.config.LeaseDuration).before(now.Time)为true的话, 就表明leader过期了。白话文的意思就是从leader上次续约完, 已经超过le.config.LeaseDuration的时间没有续约了, 所以被认为该leader过期了，这时候non-leader就可以抢占leader了。

# 4总结

leaderelection 主要是利用了k8s API操作的原子性实现了一个分布式锁，在不断的竞争中进行选举。选中为leader的进行才会执行具体的业务代码，这在k8s中非常的常见，而且我们很方便的利用这个包完成组件的编写，从而实现组件的高可用，比如部署为一个多副本的Deployment，当leader的pod退出后会重新启动，可能锁就被其他pod获取继续执行。

当应用在k8s上部署时，使用k8s的资源锁，可方便的实现高可用，但需要注意：

- 推荐使用lease或`configmap`作为资源锁，原因是某些组件（如`kube-proxy）`会去监听`endpoints`来更新节点iptables规则，当有大量资源锁时，势必会对性能有影响。

# 5参考

[https://www.cnblogs.com/zhangmingcheng/p/15846133.html]: 

