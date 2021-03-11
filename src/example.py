from src.utils import log_error_pro, log_info, log_error, log_warning, log_exception, Database, Redis
from config import CONFIG


def use_redis():
    rds = Redis(CONFIG["DATABASE"]["REDIS_URL"])
    rds.set("1", "str1")
    rds.get("2", default="null")


def use_db():
    """
    假设有如下表:
    id(primary key,自增), name(varchar(255)), height(int), weight(float), update_time(datetime, 默认插入时间)
    详细用法:
    https://dataset.readthedocs.io/en/latest/quickstart.html
    """
    db = Database(CONFIG["DATABASE"]["MYSQL_URL"])
    table = "table"
    db.insert(table, {"name": "Jack", "height": 174, "weight": 55.5})
    db.insert_many(
        table, [{"name": "Bob", "height": 181, "weight": 73.2}, {"name": "Alice", "height": 163, "weight": 50.1}],
        types={  # 可以忽略此参数,默认自动转换为数据库使用的类型
            "name": db.types.string(255),
            "height": db.types.integer,
            "weight": db.types.float,
        }
    )
    db.delete(table, name="Bob")
    db.update(table, {"id": 4, "height": 164}, keys=["id"])
    db.upsert(table, {"id": 1, "height": 175, "weight": 55.5}, keys=["id"])
    db.select(table, weight={">=": 50, }, height={"<=", 175}, order_by=["-id"])  # 体重大于50kg且身高小于175,倒序查询
    db.begin()
    try:
        db.query("INSERT INTO table2 FROM (SELECT * FROM table2 WHERE update_time>NOW() -INTERVAL 1)")
        db.commit()
    except Exception as e:
        log_error_pro("query_error", e)
        db.rollback()


def use_log():
    log_info("Start")
    try:
        log_warning("Below code has error:")
        a = 1 / 0
    except Exception as e:
        log_error("Division by 0")  # 输出单行错误信息
        log_exception(e)  # 输出堆栈错误信息
        log_error_pro("Division by 0", e)  # log_error_pro = log_error + log_exception 同时输出自定义错误消息和堆栈信息


def test_param(p1, p2=None):
    print(f"p1={p1} type(p1)={type(p1)}, p2={p2} type(p2)={type(p2)}")
