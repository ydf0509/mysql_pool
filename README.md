# 1.mysql_pool 介绍

mysql_pool是使用python通用对象池 universal_object_pool 实现的mysql连接池

mysql_pool不依赖美国大神写得Dbutils库来封装连接池，而是使用 python万能对象池 universal_object_pool 来驱动。

```
在多线程环境下操作mysql，不能多线程操作同一个connection，否则会造成事务混乱疯狂报错

要么线程内部的函数里面每次临时创建mysql连接，然后摧毁连接。反复频繁创建连接代价大

要么就是用mysql连接池，
mysql_pool是使用万能通用对象池实现的mysql连接池，完全不依赖Dbutils库来实现连接池。
```

# 2. 安装 

pip install mysql_pool

# 3 使用例子和测试代码

```python
import typing
import time
import pymysql
from threadpool_executor_shrink_able import BoundedThreadPoolExecutor
import decorator_libs
from mysql_pool import ObjectPool,PyMysqlOperator

"""
测试mysql连接池，多线程环境下必须使用连接池
"""

mysql_pool = ObjectPool(object_type=PyMysqlOperator, object_pool_size=100, object_init_kwargs={'port': 3306})


def test_update(i):
    sql = f'''
        INSERT INTO db1.table1(uname ,age)
    VALUES(
        %s ,
        %s)
    ON DUPLICATE KEY UPDATE
        uname = values(uname),
        age = if(values(age)>age,values(age),age);
    '''
    with mysql_pool.get(timeout=2) as operator:  # type: typing.Union[PyMysqlOperator,pymysql.cursors.DictCursor] #利于补全
        # operator 拥有 cursor对象的所有用法。
        print(id(operator.cursor), id(operator.conn))
        operator.execute(sql, args=(f'name_{i}', i * 4))
        print(operator.lastrowid)  # opererator 自动拥有 operator.cursor 的所有方法和属性。 opererator.methodxxx 会自动调用 opererator.cursor.methodxxx


conn = pymysql.connect(host='192.168.6.130')


def test_update_multi_threads_use_one_conn(i):
    """
    这是个大错特错的例子，多线程操作同一个连接，造成事务混乱。
    这个是个错误的例子，多线程运行此函数会疯狂报错,单线程不报错。
    这个如果运行在多线程同时操作同一个conn，就会疯狂报错。所以要么狠low的使用临时频繁在函数内部每次创建和摧毁mysql连接，要么使用连接池。
    :param i:
    :return:
    """
    sql = f'''
        INSERT INTO db1.table1(uname ,age)
    VALUES(
        %s ,
        %s)
    ON DUPLICATE KEY UPDATE
        uname = values(uname),
        age = if(values(age)>age,values(age),age);
    '''

    cur = conn.cursor()
    cur.execute(sql, args=(f'name_{i}', i * 3))
    cur.close()
    conn.commit()


thread_pool = BoundedThreadPoolExecutor(20)
with decorator_libs.TimerContextManager():
    for x in range(200000, 300000):
        thread_pool.submit(test_update, x)
        # thread_pool.submit(test_update_multi_threads_use_one_conn, x)
    thread_pool.shutdown()
time.sleep(10000)  #这个可以测试验证，此对象池会自动摧毁连接如果闲置时间太长，会自动摧毁对象
```