# chord_simulation
使用Python在本地模拟[chord (peer-to-peer)](https://en.wikipedia.org/wiki/Chord_(peer-to-peer))算法。

## Prerequisites

需求Python Version>= 3.7，可以通过`requirements.txt`安装所需依赖。

```sh
pip install -r requirements.txt
```

主要需要安装如下第三方库：

-   [Thriftpy/thriftpy2](https://github.com/Thriftpy/thriftpy2)：Thrift的Python实现，用于RPC通信
-   [Delgan/loguru](https://github.com/Delgan/loguru)：开箱即用的Python日志管理框架

