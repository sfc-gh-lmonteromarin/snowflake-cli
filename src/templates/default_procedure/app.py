from __future__ import annotations

import sys

from snowflake.snowpark import Session


def hello() -> str:
    return "Hello World!"


# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == "__main__":
    from snowcli.config import cli_config

    session = Session.builder.configs(**cli_config.get_connection("dev")).create()
    if len(sys.argv) > 1:
        print(hello(*sys.argv[1:]))  # type: ignore
    else:
        print(hello())  # type: ignore
    session.close()
