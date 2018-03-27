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

 ### 物品分类
 现有这么一些输入数据
 ```
 1        0        家电
 2        0        服装
 3        0        食品
 4        1        洗衣机
 5        1        冰箱
 6        2        男装
 7        2        女装
 8        3        零食
 9        3        水果
 10        4        美的
 11        5        海尔
 12        6        衬衫
 13        7        蓬蓬裙
 14        8        牛奶
 15        14        特仑苏
 ```
 要求输出数据为
 ```
 家电        洗衣机-美的    
 家电        冰箱-海尔      
 服装        男装-衬衫
 服装        女装-蓬蓬裙
 食品        零食-牛奶-特仑苏
 食品        水果
 ```
 
 ## TopN
 
 ### User Location
 现有如下数据文件需要处理: 格式:CSV
 数据样例:
 ```
 user_a,location_a,2018-01-01 08:00:00,60 
 user_a,location_a,2018-01-01 09:00:00,60
 user_a,location_b,2018-01-01 10:00:00,60
 user_a,location_a,2018-01-01 11:00:00,60
 
```
 字段:用户 ID，位置 ID，开始时间，停留时长(分钟) 
 
 数据意义:某个用户在某个位置从某个时刻开始停留了多长时间 
 
 处理逻辑: 对同一个用户，在同一个位置，连续的多条记录进行合并
 合并原则:开始时间取最早的，停留时长加和
 
 要求:请编写 MapReduce 程序实现 
 其他:只有数据样例，没有数据。