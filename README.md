# dianshang
尚硅谷电商项目，自己瞎写着玩

包含的模块：
1. 日活统计（rihuo）:
kafka (登录日志) ----> flink(去重) ----> es （结果）

2. 首单分析(shoudan):
mysql（订单信息）-----> flink cdc ---->kafka(消息保存)  -----> flink将消息连接hbase，查询是否首单------>将首单数据写入es 
