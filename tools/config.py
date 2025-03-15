import os

import environ


class CustomEnvironment:
    env = environ.Env()

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    environ.Env.read_env(os.path.join(BASE_DIR, ".env"))

    _aws_password = env.str("AWS_PASSWORD")
    _aws_user = env.str("AWS_USER")

    @classmethod
    def get_aws_password(cls) -> str:
        return cls._aws_password

    @classmethod
    def get_aws_user(cls) -> str:
        return cls._aws_user
