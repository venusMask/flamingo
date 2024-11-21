# Flamingo

> *A high-performance KV separated LSM storage engine.*

# 架构



# 数据存储

对于需要kv分离的节点:

			`		[total_size, delete_flag, store_mode(true), key_size, key_value, file_id, offset]`

对于不需要分离的节点：

			`[total_size, delete_flag, store_mode(false), key_size, key_value, value_size, value]`





# 读写流程

<img src="./flamingo.assets/截屏2024-11-21 16.46.57.png" alt="截屏2024-11-21 16.46.57" style="zoom: 33%;" />



## *1: MemTable*

MemTable中主要的数据存储是跳表，跳表中的数据一旦达到阈值则将跳表包装成不可变对象放入任务队列中，然后等待调度，一旦放入任务队列之后就产生新的MemTable接受用户请求。



## *2: MetaInfo*

> 元信息管理。管理所有的SSTableInfo。

每一个sst被添加到MetaInfo或者是被从MetaInfo中删除的时候都需要把MetaInfo重新刷新到磁盘上。以防止元信息丢失产生错误。

MetaInfo会同时被多个模块访问，MetaInfo模块需要考虑并发访问的问题。

LevelMetaInfo

添加SST

1. 获取写锁，然后写入数据，释放写锁。

读取SST

1. 获取读锁，然后查询需要的SST，返回这些SST的拷贝。释放读锁。

删除SST

1. 获取写锁，然后删除SST，释放写锁。



## *3: Compact*

compact分为第0层的合并和其余层的合并。内存中的数据达到阈值之后都会被刷新到第0层，所以第0层中的数据是key重叠的。

我们假设写入的元信息所在层级为 *level*. 一旦 *level* 所在层的 *sst* 数量达到阈值，则从当前层挑选出需要合并的 *upper_sst*，然后从下一层挑选有key重叠的 *lower_sst* 然后进行合并。合并的结果写入 *level + 1* 层。

注意:

1. 倒数第二层的数据跟最后一层的数据合并之后重新写入最后一层。(没有实现)。
2. 合并完成之后要删除 *upper_sst & lower_sst* 的元信息。(MetaInfo的并发访问问题) (删除考虑是否是一个异步的动作)。
3. 在写入 *level + 1* 层的时候有可能触发下一层的合并，并且链式触发下去。(考虑新的Compact调度)。



### 1: *Compact Alg*

> *File Merge Sort* 文件归并排序算法。





# 基础操作

## 1: 写入

1. 数据请求首先被写入 v_log，数据格式为:  

​             `[total_size, delete_flag, key_size, key_value, value_size, value]`

​        返回写入的文件地址 `[file_id, offset]`。

2. 然后组装跳表节点，数据格式为

​		对于需要kv分离的节点:

​			`		[total_size, delete_flag, store_mode(true), key_size, key_value, file_id, offset]`

对于不需要分离的节点：

​			`[total_size, delete_flag, store_mode(false), key_size, key_value, value_size, value]`

然后将数据写入跳表，如果写入成功则返回成功响应给客户端，否则返回异常。

3. 校验跳表的节点量是否超过了阈值，如果超过了阈值则刷新内存数据到磁盘，步骤如下：
   1. 将当前MemTable封装成刷新任务添加到任务队列。等待刷新。
      1. 首先将跳表的状态切换到不可变状态。
      2. 然后获取即将需要写入的层级信息SSTableInfo。
      3. 将数据刷新到SSTableInfo所代表的文件中。
      4. 添加SSTableInfo到MetaInfo中。接下来的步骤参数元信息管理。
   2. 将MemTable切换到新的跳表继续接受用户的请求。






