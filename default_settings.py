# -*- coding:utf-8 -*-


LOG_LEVEL = 'DEBUG'      # 日志级别

LOG_STDOUT = True        # 是否标准到控制台

LOG_JSON = False         # 是否输出json格式

LOG_DIR = "logs"         # log文件目录

LOG_MAX_BYTES = '10MB'   # 每个log最大大小

LOG_BACKUPS = 5          # log备份数量

TO_KAFKA = False         # 是否发送到kafka

KAFKA_HOSTS = "192.168.200.90:9092" # kafka地址

TOPIC = "jay-cluster-logs"        #kafka topic