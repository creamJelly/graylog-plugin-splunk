Graylog plugin for Splunk with UDP
=========================
将发送到graylog上的客户端tlog内容以UDP协议转发给指定的tlog服务器，写入当前的tlog日志文件中

##输出参数
创建output时，选择splunk output -> launch new output。填写相关信息，只支持udp。params字段填写对应的tlog结构，格式如FlowName=xxx,tlog字段=xxxx,tlog字段=xxx……………………
每个output暂时只能对应一条tlog
