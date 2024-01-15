# 6.824

耗时近两个月，终于完成了6.824的四个lab，虽然debug的过程很痛苦，但是最终完成的时候还是有一些成就感的。

只能说不愧是MIT，在保证lab难度的同时，提供了很完善的框架代码，测试用例以及实验指导，让学生能够专注于解决核心问题，做完lab后会对分布式系统有一个更加全面的了解，对容错、复制、一致性等概念有新的认识。

lab使用的语言是go，刚开始还觉得为了一个lab去学习一种新的语言是否有点浪费。后来觉得go这门语言确实适合于分布式系统，go语言的goroutine和channel大大降低了多线程编程的难度，并且gc机制让我们不需要关注内存泄露等问题。该lab其实用不到go中什么复杂的语法，简单语法去[官网](https://go.dev/tour/list)看个一天左右就够用了。而且go的-race是真的好用。

四个lab中，lab1是完成一个简易单词统计的MapReduce，难度其实很低，很多框架代码都给出了，而且有论文作为参照，lab1的目的主要也是为了熟悉下go语言，再熟悉一下lab的rpc库；lab2的难度一下就上来了，实现raft算法，这个lab最好的参考资料其实就是[原论文](http://nil.csail.mit.edu/6.824/2022/papers/raft-extended.pdf)，论文中基本将所有细节都提到了，但是在实现时还是会有各种细节上的问题，得慢慢debug，难度还是非常高的；lab3则是实现一个有容错的kv service，只有唯一的一个副本组，持有整个数据库，这个lab没有论文作为参考，很多地方需要自己去思考和设计，也是有一定难度的，而且严格依赖于lab2中实现的raft，很多raft的问题会暴露在lab3；lab4是在lab3基础上实现分片的kv service，有多个副本组，负责不同的分片，需要实现分片控制器和副本组服务器，lab4的设计空间更大，实验过程中也参考了很多博客中别人的思路，整体难度感觉不逊于lab2。

整个lab最难的地方在于debug的过程，分布式系统的debug只能靠打印log，必须将最有用的信息简洁明了的输出，否则可能一个测试就输出十几万行茫茫多的log，很难定位错误。在这方面的优化越早考虑越好，最好是做一些日志美化的处理，我就是因为没做这方面的处理导致debug的过程非常痛苦。可以参考这篇文章[Debugging by Pretty Printing](https://blog.josejg.com/debugging-pretty/).

## 代码

四个lab的代码都在这个仓库里，lab2,3,4对应目录下有并行测试的python脚本，三个lab都通过5000次测试无错，lab4B通过10000次。

测试命令为 

`python ./dstest.py -p 并行数 -n 总次数`

## 笔记

四个lab都做了比较详细的笔记

[lab1笔记](./note/lab1.md)

[lab2笔记](./note/lab2.md)

[lab3笔记](./note/lab3.md)

[lab4笔记](./note/lab4.md)

## 参考资料
[MapReduce论文](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf)

[raft论文](http://nil.csail.mit.edu/6.824/2022/papers/raft-extended.pdf)

[raft实验学生指引](https://thesquareplanet.com/blog/students-guide-to-raft/)

[并行测试脚本](https://gist.github.com/JJGO/0d73540ef7cc2f066cb535156b7cbdab) 该脚本不仅能并行多次测试，而且还能只捕获出错测试的log，避免产生太多log文件，是MIT的助教提供的。并且助教在这篇[文章](https://blog.josejg.com/debugging-pretty/)中分享了如何输出信息明确的log并且提供了美化log的python代码。但可惜我发现的时候已经是lab末期了，所以就只使用了一下并行测试脚本，要是早点看到这篇文章的话debug过程可能会少很多痛苦。

[知乎文章  MIT 6.824 (现6.5840) 通关记录](https://zhuanlan.zhihu.com/p/631386296) 这篇文章写的很不错，在自己写lab的过程中参考了很多，其实也参考了很多别的博客，就不一一列出来了。

[deadlock](https://github.com/sasha-s/go-deadlock) go检测deadlock的第三方库，很有用，可以在实验初期将所有的sync.Mutex换成deadlock.Mutex，当排查完死锁问题后，再换回sync.Mutex。

