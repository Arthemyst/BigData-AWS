import os

import environ


class CustomEnvironment:
    env = environ.Env()

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    environ.Env.read_env(os.path.join(BASE_DIR, ".env"))

    _aws_access_key = env.str("AWS_ACCESS_KEY")
    _aws_key_id = env.str("AWS_KEY_ID")

    @classmethod
    def get_aws_password(cls) -> str:
        return cls._aws_access_key

    @classmethod
    def get_aws_user(cls) -> str:
        return cls._aws_key_id
