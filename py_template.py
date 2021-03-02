import argparse
import os
import importlib


def call_method(module_path: str, method: str, param: str = None):
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f"策略模块{module_path}不存在")
    else:
        if param:
            getattr(module, method)(param)
        else:
            getattr(module, method)()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-server", help="执行的服务名称")
    parser.add_argument("-module", help="执行的参数模型名称")
    parser.add_argument("-strategy", help="执行的策略名称")
    parser.add_argument("-tools", help="执行的额外脚本工具")
    parser.add_argument("-mode", help="模式(test或master)")
    parser.add_argument("-param", help="执行的策略或服务参数ID")
    args = parser.parse_args()
    if args.mode not in ("test", "master"):
        raise ValueError(f"mode设置错误({args.mode})必须为test或master")
    os.environ["MARKET_MAKER_ROOT_MODE"] = args.mode
    if getattr(args, "server"):
        os.environ["LOG_DIR_NAME"] = f"{args.server}"
        module_path = f"src.server.{args.server}"
        print(f"开启API服务{module_path}")
        call_method(module_path, "main", getattr(args, "param"))
    elif getattr(args, "module"):
        os.environ["LOG_DIR_NAME"] = f"{args.module}_{args.param}"
        module_path = f"src.module_param.{args.module}"
        print(f"开启调参服务{module_path}")
        call_method(module_path, "main", args.param)
    elif getattr(args, "tools"):
        os.environ["LOG_DIR_NAME"] = args.tools
        module_path = f"tools.{args.tools}"
        print(f"执行脚本{module_path}")
        if getattr(args, "param"):
            call_method(module_path, "main", args.param)
        else:
            call_method(module_path, "main")
    else:
        os.environ["LOG_DIR_NAME"] = f"{args.strategy}_{args.param}"
        module_path = f"src.strategy.{args.strategy}"
        print(f"执行策略{module_path}")
        call_method(module_path, "main", args.param)


if __name__ == "__main__":
    main()
