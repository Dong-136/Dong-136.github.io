---
title: golang
summary: go语言使用时需要注意的事项
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

