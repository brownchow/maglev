# Google Maglev Hash 算法的 Go 实现



## Maglev是什么？

在分布式系统中做**负载均衡**用，**一致性哈希算法**。

maglev有如下特点：

1、流量均匀分配

2、连接一致性（同一连接的数据包会被转发到相同的服务器）

3、对小数据包有高吞吐能力

[论文地址](http://static.googleusercontent.com/media/research.google.com/zh-TW//pubs/archive/44824.pdf)



使用的三方库： siphash，sipHash通过让输出随机化，减少 Hash Flooding 攻击



## 总结：

1、首先哈希表的数量m 必须是素数

2、可以与之前的 consistent 项目对照着看



## 参考资料：

https://manjusaka.itscoder.com/posts/2020/05/23/a-simple-introduction-about-maglev/

https://tech.meituan.com/2017/01/05/mgw.html

https://www.youtube.com/watch?v=_GRM1Ij_3t0

https://www.youtube.com/watch?v=jk6oiBJxcaA

https://writings.sh/post/consistent-hashing-algorithms-part-4-maglev-consistent-hash

https://www.jianshu.com/p/9a9b269e68f7