from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

## TODO! How does harlequin do username/password?
username = TextOption(
    name="username",
    description="Help text goes here",
    short_decls=["-u"],
)

account = TextOption(
    name="account",
    description="Help text goes here",
    short_decls=["-A"],
)

hostname = TextOption(
    name="hostname",
    description="Help text goes here",
    short_decls=["-h"],
)

password = TextOption(
    name="password",
    description="Help text goes here",
    short_decls=["-p"],
)

role = TextOption(
    name="role",
    description="Help text goes here",
    short_decls=["-r"],
)

warehouse = TextOption(
    name="warehouse",
    description="Help text goes here",
    short_decls=["-w"],
)

database = TextOption(
    name="database",
    description="Help text goes here",
    short_decls=["-d"],
)

schema = TextOption(
    name="schema",
    description="Help text goes here",
    short_decls=["-s"],
)
SNOWFLAKEADAPTER_OPTIONS = [username, account, password, hostname, role, warehouse, database, schema]
