# MapReduce

一些MapReduce程序，涵盖一些可能出现在面试中的问题

本地环境运行时请更改Hadoop配置和文件输入输出目录

## Basic

### 互粉好友
/src/main/java/com/hosuke/mapreduce/basic/Follows.java

```
 A:B,C,D,F,E,O
 B:A,C,E,K
 C:F,A,D,I
 D:A,E,F,L
 E:B,C,D,M,L
 F:A,B,C,D,E,O,M
 G:A,C,D,E,F
 H:A,C,D,E,O
 I:A,O
 J:B,O
 K:A,C,D
 L:D,E,F
 M:E,F,G
 O:A,H,I,J,K
```

给出用户与粉丝列表，求互粉好友对

### 共同好友列表
/src/main/java/com/hosuke/mapreduce/basic/MutualFriends.java
```
 A:B,C,D,F,E,O
 B:A,C,E,K
 C:F,A,D,I
 D:A,E,F,L
 E:B,C,D,M,L
 F:A,B,C,D,E,O,M
 G:A,C,D,E,F
 H:A,C,D,E,O
 I:A,O
 J:B,O
 K:A,C,D
 L:D,E,F
 M:E,F,G
 O:A,H,I,J,K
```
 以上是数据：
 A:B,C,D,F,E,O
 表示：B,C,D,E,F,O是A用户的好友。

 求所有两两用户之间的共同好友