from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

## TODO! How does harlequin do username/password?
foo = TextOption(
    name="foo",
    description="Help text goes here",
    short_decls=["-f"],
)

SNOWFLAKEADAPTER_OPTIONS = [foo]
