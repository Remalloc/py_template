import os
import click


@click.group(chain=True)
def main():
    pass


@click.command()
@click.option("--p2", default=None, help="number of greetings")
@click.argument("p1")
def run(p1, p2):
    import src.example
    src.example.test_param(p1, p2)


@click.command()
@click.option("--config", help="配置文件名")
@click.argument("config")
def set_config(config):
    os.environ["CONFIG_NAME"] = config


if __name__ == "__main__":
    """
    python py_template.py config example run 123 --p2 456
    1. 配置使用example.toml
    2. run 参数p1=123 p2=456
    """
    main.add_command(set_config, "config")
    main.add_command(run)
    main()
