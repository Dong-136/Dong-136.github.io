---
title: golang语言用法及实现原理
summary: 包含了golang使用时需要注意的事项和实现原理
tags:
  - kubernetes
date: 2025-06-01
toc: true
# external_link: http://github.com
---

# 1. select在for内部时，break不会跳出for循环

```go
func main() {
	ch := make(chan int)
	go testSelectFor(ch)
	ch <- 1
	ch <- 2
	close(ch)
	time.Sleep(1 * time.Second)
}

func testSelectFor(ch chan int) {
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				fmt.Printf("channel closed: %d\n", v)
				time.Sleep(1000000 * time.Microsecond)
				break //continue
			}
			fmt.Printf("channel received: %d\n", v)
		}
		fmt.Printf("for Remainder\n")
	}
}
```

```shell
# break的输出
channel received: 1
for Remainder
channel received: 2
for Remainder
channel closed: 0
for Remainder
# continue的输出
channel received: 1
for Remainder
channel received: 2
for Remainder
channel closed: 0
channel closed: 0
```

这里对break和continue作一下区分：

break可以作用于select，跳出select执行for循环，select的case的break后面的语句不会执行，会执行for内部select模块后面的语句。

continue只作用于for循环，意味着continue后面的select的case和for内部在select后面的语句都不会执行。

# 2.go的"golang.org/x/net/bpf"包使用注意事项

最近在看antrea源码，在PacketCapture抓包功能里使用了bpf来实现抓包的功能，在看实现原理时发现是自己拼装的Instruction，处于好奇，就自己实现一个抓包工具，通过tcpdump显示编译后的 BPF（Berkeley Packet Filter）指令集，然后自己转化成bpf.Instructions，示例如下：

```shell
tcpdump -i pod-test-eec684 -d "tcp and src host 172.18.136.93 and dst host 10.124.0.31"
(000) ldh      [12]
(001) jeq      #0x86dd          jt 10   jf 2
(002) jeq      #0x800           jt 3    jf 10
(003) ldb      [23]
(004) jeq      #0x6             jt 5    jf 10
(005) ld       [26]
(006) jeq      #0xac12885d      jt 7    jf 10
(007) ld       [30]
(008) jeq      #0xa7c001f       jt 9    jf 10
(009) ret      #262144
(010) ret      #0
```

这里的**JumpTrue**和**JumpFalse**需要注意，tcpdump写的是跳转的序号，就是最前面括号内的标号，而go的bpf.instruction则使用的是相对顺序，因为包要求指令不能够往回跳（防止出现无限循环，确保内核安全），所以具体要跳的数字要根据要跳的位置来计算，bpf的包会对指令检查，如果跳转的顺序越界，就会报（invalid argument）。

正确的转换关系如下：

```go
[]bpf.Instruction{
		// 加载以太网协议类型 (偏移 12 字节)
		bpf.LoadAbsolute{Off: 12, Size: 2},
		// 检查以太网类型是否为 ARP (0x86dd)
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0x86dd, SkipTrue: 8, SkipFalse: 0},
		// 检查以太网类型是否为 IPv4 (0x0800)
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0x0800, SkipTrue: 0, SkipFalse: 7},
		// 加载 IP 协议 (偏移 23 字节)
		bpf.LoadAbsolute{Off: 23, Size: 1},
		// 检查协议是否为 TCP (0x06)
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 6, SkipTrue: 0, SkipFalse: 5},
		// 加载源 IP 地址 (偏移 26 字节)
		bpf.LoadAbsolute{Off: 26, Size: 4},
		// 检查源 IP 地址是否为 172.18.136.93 (0xac12885d)
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0xac12885d, SkipTrue: 0, SkipFalse: 3},
		// 加载目的 IP 地址 (偏移 30 字节)
		bpf.LoadAbsolute{Off: 30, Size: 4},
		// 检查目的 IP 地址是否为 10.124.0.31 (0xa7c001f)
		bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0xa7c001f, SkipTrue: 0, SkipFalse: 1},
		// 通过：返回整个包
		bpf.RetConstant{Val: 0xFFFF},
		// 拒绝：返回 0
		bpf.RetConstant{Val: 0},
	}
```

# 3.golang GPM模型

golang的的特点就是天生支持高并发，而高并发的实现则是依赖goroutinue来实现的，golang可以创建成千上万个goroutine来处理任务，将这些goroutinue分配、负载、调度到处理器上采用的是GPM模型。

## 什么是goroutinue?

GoRoutinue=golang + coroutine，Goroutinue是golang实现的协程，是用户级线程，goroutinue具有以下优点：

1. 相比于线程，其启动代价很小，以很小的栈空间启动（2kb左右）
2. 栈空间可以进行伸缩，最大可以扩充到GB级别
3. 能够利用多核处理器，实现进程内线程的并发处理
4. 工作在用户态，需要的资源少，上下文切换开销低

## 进程、线程、Goroutinue之间的关系

在仅支持进程的操作系统中，进程是资源分配和独立调度的基本单位，在支持线程的操作系统中，进程是资源分配的基本单位，线程是独立调度的基本单位，一个进程内可以启动多个线程，在同一个进程内，线程共享进程内部的资源（共享那些资源？），同一个进程内部，线程的切换不会引起进程的切换，不同进程内的线程切换会引起进程的切换。

线程的创建、调度、管理等采用的方式为线程模型，线程模型一般分为以下三种：

1. 内核级线程模型（kernel Level Thread）
2. 用户级线程模型（User Level Thread）
3. 两级线程模型，也称混合型线程模型

三者的区别是用户态线程与内核调度实体（KSE，Kernel Scheduling Entity）的对应关系，内核调度实体是可被操作系统调度的对象实体，是操作系统内核的最小调度单元，可以理解为内核级线程。

用户态线程即是协程，由应用程序创建和管理，线程由CPU调度，是抢占式的，协程由用户态程序调度，是协作式的，一个协程让出CPU后，才能执行下一个协程。

| 特性               | 用户级线程                                             | 内核级线程                                           |
| ------------------ | ------------------------------------------------------ | ---------------------------------------------------- |
| 创建者             | 应用程序                                               | 内核                                                 |
| 操作系统是否能感知 | 否                                                     | 是                                                   |
| 开销成本           | 创建成本低，上下文切换成本低，上下文切换不需要硬件支持 | 创建成本高，上下文切换成本高，上下文切换需要硬件支持 |
| 如果线程阻塞       | 整个进程将会被阻塞，不能利用多核处理器发挥并发优势     | 其他线程可以继续执行，进程不会阻塞                   |
| 案例               | Java thread，POSIX threads                             | Window Solaris                                       |

### 内核级线程模型

![](/Volumes/T7/study/hugo/Dong-136.github.io/static/images/ult_klt_1_1.jpg)

内核级线程与用户态线程是1:1的关系，线程的创建、销毁、调度都是由用户态程序通过系统调用交由操作系统内核来完成，每个用户线程都会绑定到一个用户态线程，两者的生命周期相同，操作系统的内存管理和调度子系统必须要考虑到大量的用户级线程。

优点：

1. 在多处理系统中，可以利用多核优势，并发处理同一个进程的线程任务
2. 线程阻塞不会导致进程阻塞，可以切换到其他线程继续执行
3. 当一个线程阻塞时，内核可以选择运行另一个进程的线程，而用户空间中实现的线程中，运行时系统始终运行自己进程中的线程。

缺点：

- 线程的创建和销毁都需要内核参与，开销较大

### 用户级线程模型

![](/Volumes/T7/study/hugo/Dong-136.github.io/static/images/ult_klt_n_1.jpg)

用户线程模型中的用户线程和内核线程是n:1的关系，线程的创建、调度、销毁都是在用户态完成的，由应用程序的线程库来完成，内核对这些是无感知的，内核此时的调度都是基于进程的，线程的并发处理从宏观来看，任意时刻每个进程只能够有一个线程在运行，且只有一个处理器内核会被分配给该进程。

用户级线程的优点：

- 创建、销毁线程和切换线程的代价都比内核级线程少得多，因为保存线程状态的过程和调用程序都只是本地过程
- 线程能够利用的表空间和堆栈空间比内核级线程多

缺点：

- 无法利用多核处理器的并发优势，线程阻塞将导致整个进程阻塞，同一时间只能有一个线程在内核中执行。

### 两级线程模型

![](/Volumes/T7/study/hugo/Dong-136.github.io/static/images/ult_klt_n_m.jpg)

两级线程模型中用户线程和内核线程是多对多的关系（N:M），线程的创建和调度是在应用程序中进行的，一个应用程序的多个用户级线程被绑定到一些内核级线程上。

优点：

- 既可以利用多核处理器的并发处理能力（相对于用户级线程），又减少了上下文开销（相对于内核级线程）
- 进程内的一个线程阻塞，不会导致整个进程阻塞

## Golang的线程模型

![](/Volumes/T7/study/hugo/Dong-136.github.io/static/images/golang_schedule_status.jpeg)

从上图可知：

1. 每个P都有一个本地队列，局部队列保存待执行的goroutinue(流程2)，当M绑定的P的局部队列已经满了的时候就会把goroutinue放入到全局队列中（2-1）
2. 每个P和一个M绑定，M是真正的执行P中goroutinue的实体（3），M从绑定的P中的局部队列获取G来执行，若本地队列中没有可执行的goroutinue，则从全局队列中获取可执行的goroutinue，若全局队列没有则从别的M绑定的P中偷取goRoutinue来执行，这个过程称为work stealing
3. 当G因为系统调用阻塞M时，P会和M解绑即hand off，并寻找新的idle的M进行绑定，来继续处理队列中的G
4. 当G因为channel或网络I/O阻塞时，不会阻塞M，M会寻找其他可执行的G来继续工作，当这个G恢复后，会重新进入P队列等待执行。

[https://go.cyub.vip/gmp/gmp-model.html]()

## Golang的defer return

defer语句是一个后进先出（LIFO）的顺序（有博主说是以链表的形式进行保存，后加入的defer函数会放入到链表的头部，然后Goroutinue结束时会逐个执行；说是栈的博客并没有看到源码分析，或许是人云亦云？），通常用于释放资源、锁、连接。

return在go语言中并不是一个原子操作，包括赋值、返回两个操作。

三者的顺序为：return先将值赋值给返回值，然后执行defer操作，最后函数携带返回值退出。  即：return赋值->defer->return返回。这也就是为什么defer可以更改返回值，出现与“预想结果”不一样的原因。然而，函数的返回值是有名的和无名的结果又不一样。

无名返回值函数：

```go
func main() {
	t := test()
	fmt.Println(t)
}

func test() int { //无名返回
	i := 9
	defer func() {
		i++
		fmt.Println("defer1=", i)
	}()

	defer func() {
		i++
		fmt.Println("defer2=", i)
	}()

	return i
}
```

返回结果：

```go
defer2= 10
defer1= 11
9

Process finished with the exit code 0
```

分析：defer2和defer1都对i执行了加一操作，打印出的结果也表示执行成功，然而按照前面的分析，这里返回的值却没有变，仍然是9，说明return赋值这一步并没有使用“i”这个变量作为返回变量，而是赋值给了一个临时变量，后续defer的操作只影响函数内部的i值，不影响被赋值的返回值。相当于：

```go
var tmp int
tmp = i
return tmp
```

有名字的返回值函数：

```go
func test() (i int) { //有名返回i
	i = 9
	defer func() {
		i++
		fmt.Println("defer1=", i)
	}()

	defer func() {
		i++
		fmt.Println("defer2=", i)
	}()

	return
}
```

输出结果：

```go
defer2= 10
defer1= 11
11

Process finished with the exit code 0
```

分析：输出的函数的返回值为11，表明defer对变量i的加一操作，是对return赋值后的变量操作，由于这里函数直接声明了返回变量名，所以操作的都是同一个变量，
