from __future__ import annotations

import fonts
from PyRTF.Elements import StyleSheet
from snowflake.snowpark import Session


def hello_function(name: str) -> str:
    return f"{StyleSheet.__str__} {name}"