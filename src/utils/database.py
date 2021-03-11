from typing import Iterable, Dict, Callable

import dataset
import redis
import simplejson as json

from src.utils import log_error_pro, log_error


class Redis(object):
    def __init__(self, url: str):
        self.conn_redis = redis.from_url(url)

    def push(self, name: str, values: Iterable) -> None:
        """
        依次将字典序列化后值推入对应的list键中
        :param name: list键名
        :param values: 推入的数据[{"a":1,"b":2}...]
        :return: None
        """
        for data in values:
            self.conn_redis.rpush(name, bytes(json.dumps(data, use_decimal=True), "UTF-8"))

    def lpop(self, key: str) -> dict:
        """
        弹出对应的list键的值,并将字典反序列化
        :param key:
        :return: {"a":1,"b":2}
        """
        value = self.conn_redis.lpop(key)
        if value:
            return json.loads(value.decode(), use_decimal=True)
        else:
            return {}

    def blpop(self, key: str) -> dict:
        """
        阻塞的弹出对应的list键的值,并将字典反序列化
        :param key:
        :return: {"a":1,"b":2}
        """
        value = self.conn_redis.blpop(key)
        return json.loads(value[1].decode(), use_decimal=True)

    def set(self, name: str, value: str):
        """
        设置字符串键值
        :param name: 键
        :param value: 值
        :return: Redis返回值
        """
        return self.conn_redis.set(name, str(value))

    def get(self, name: str, default=None) -> str:
        """
        获取字符串键的值
        :param name: 键
        :param default: 如果未找到返回默认值
        :return: 字符串或默认值
        """
        value: bytes = self.conn_redis.get(name)
        if value is not None:
            return value.decode()
        else:
            return default

    def hget(self, name: str, key: str, vtype: [type, Callable] = str, default=None):
        """
        获取hash对应的值并转换类型
        :param name: hash键名
        :param key: hash内键名
        :param vtype: 值类型
        :param default: 没有获取到值后的默认返回值
        :return: vtype or None
        """
        r = self.conn_redis.hget(name, key)
        if r is None:
            return default
        else:
            return vtype(r.decode())

    def hset(self, name: str, key: str, value: object) -> int:
        """
        设置hash值并自动转化为bytes类型
        :param name: hash键
        :param key: hash内部键
        :param value: 任意值
        :return: 创建了新字段返回1,其他情况返回0
        """
        return self.conn_redis.hset(name, key, bytes(str(value), "UTF-8"))

    def hgetall(self, name: str, value_type: Dict[str, Callable] = None) -> dict:
        """
        获取hash键的所有值并转换为对应类型
        :param name: hash键
        :param value_type: 参数类型对应,不提供返回原始数据
        {
            "键名":类型(Callable)
        }
        :return: 原始数据字典 或 转换类型后字典
        """
        values = self.conn_redis.hgetall(name)
        if value_type:
            result = {}
            for param, type_ in value_type.items():
                result[param] = type_(values[param.encode()].decode())
            return result
        else:
            return {k.decode(): v.decode() for k, v in values.items()}

    def hmset(self, name: str, value: dict):
        value = {k: str(v) for k, v in value.items()}
        return self.conn_redis.hmset(name, value)

    def hdel(self, name: str, *keys: str):
        return self.conn_redis.hdel(name, *keys)

    def delete(self, *names: str):
        return self.conn_redis.delete(*names)

    def exists(self, name: str):
        return self.conn_redis.exists(name)

    def scan_iter(self, match=None, count=None):
        return self.conn_redis.scan_iter(match, count)

    def keys(self, pattern: str) -> list:
        return self.conn_redis.keys(pattern=pattern)


class Database(object):
    # https://dataset.readthedocs.io/en/latest/quickstart.html
    ERROR_TAG = "utils_db_error"

    def __init__(self, url: str, db: dataset.Database = None):
        if db:
            self._db = db
        else:
            self._db = dataset.connect(
                url, engine_kwargs={"isolation_level": "AUTOCOMMIT", "pool_recycle": 3600, "pool_pre_ping": True}
            )
        self.types = self._db.types

    def log_error_pro(self, e: Exception):
        log_error_pro(self.ERROR_TAG, e)

    def log_error(self, e: Exception):
        log_error(self.ERROR_TAG, e)

    def query(self, sql: str, is_handle_error: bool = False):
        """
        执行自定义sql语句
        :param sql: sql语句
        :param is_handle_error: True:当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return: 生成器
        """
        try:
            return self._db.query(sql)
        except Exception as e:
            if is_handle_error:
                self.log_error_pro(e)
            else:
                raise

    def select(self, table_name: str, **filters) -> list:
        """
        查询select("test",id=0,type=0)
        :param table_name: 表名
        :param filters: 约束, 如: col={">=":1}
        :return: 结果元组
        """
        if filters:
            return list(self._db[table_name].find(**filters))
        else:
            return list(self._db[table_name].all())

    def is_exist(self, table_name: str, **filters) -> dict:
        """
        查询值是否存在,如果查询失败会抛出异常
        :param table_name: 表名
        :param filters: 约束
        :return: None or dict
        """
        table = self._db[table_name]
        return table.find_one(**filters)

    def insert(self, table_name: str, data: dict, is_handle_error=False) -> bool:
        """
        插入数据
        :param table_name: 表名
        :param data: 字典数据{"字段名":值}
        :param is_handle_error: True:当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return: 插入结果
        """
        table = self._db[table_name]
        try:
            return True if table.insert(data) else False
        except Exception as e:
            if is_handle_error:
                self.log_error_pro(e)
                return False
            else:
                raise

    def insert_ignore(self, table_name: str, data: dict, keys: list, is_handle_error=False) -> bool:
        """
        如果关键字不相同就插入数据
        :param table_name: 表名
        :param data: 字典数据{"字段名":值}
        :param keys: 关键字,关键字相同则认为数据相同
        :param is_handle_error: True:当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return: 插入结果
        """
        table = self._db[table_name]
        try:
            return True if table.insert_ignore(data, keys, False) else False
        except Exception as e:
            if is_handle_error:
                self.log_error(e)
                return False
            else:
                raise

    def insert_many(self, table_name: str, data: list, types=None, is_handle_error=False) -> bool:
        """
        批量插入多值
        :param table_name: 表名
        :param data: 数据列表,每行为一个字典[{"col1":v1,"col2":v2},{"col1":v1,"col2":v2}...]
        :param types: 指定数据类型否则自动转换为数据表中的类型, {"col1":db.types.text, "col2":db.types.string}
        :param is_handle_error: 当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return: 插入结果
        """
        table = self._db[table_name]
        try:
            table.insert_many(data, ensure=False, types=types)
        except Exception as e:
            if is_handle_error:
                self.log_error(e)
                return False
            else:
                raise
        return True

    def upsert(self, table_name: str, data: dict, keys: list, is_handle_error=False) -> bool:
        """
        插入更新,不存在就插入,存在就更新
        :param table_name: 表名
        :param data: 插入数据
        :param keys: 关键字,关键字相同则认为数据相同
        :param is_handle_error: 当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return:
        """
        table = self._db[table_name]
        try:
            return True if table.upsert(data, keys, ensure=False) else False
        except Exception as e:
            if is_handle_error:
                self.log_error(e)
                return False
            else:
                raise

    def update(self, table_name: str, data: dict, keys: list, is_handle_error=False):
        """
        更新数据
        :param table_name: 表名
        :param data: 字典数据{"字段名":值}
        :param keys: 索引字段["ID",...]
        :param is_handle_error: 当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return:更新结果
        """
        table = self._db[table_name]
        try:
            return True if table.update(data, keys) else False
        except Exception as e:
            if is_handle_error:
                self.log_error(e)
                return False
            else:
                raise

    def delete(self, table_name: str, is_handle_error=False, **filters) -> int:
        """
        删除数据
        :param table_name: 表名
        :param filters:"字段名"=值
        :param is_handle_error: 当执行SQL时发生错误捕获异常日志返回失败, False:发生错误时直接向调用者抛出异常
        :return: 删除行数
        """
        table = self._db[table_name]
        try:
            return table.delete(**filters)
        except Exception as e:
            if is_handle_error:
                self.log_error(e)
                return 0
            else:
                raise

    def __enter__(self):
        """Start a transaction."""
        return self._db.__enter__()

    def __exit__(self, error_type, error_value, traceback):
        """End a transaction by committing or rolling back."""
        return self._db.__exit__(error_type, error_value, traceback)

    def begin(self):
        self._db.begin()

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

    def close(self):
        self._db.close()
