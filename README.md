# Flamingo
> A high-performance KV separated LSM storage engine.

# 一: Write
1. 数据首先被写入v_log文件, 返回写入地址, 写入数据的格式为
