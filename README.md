# OSQuery Tables (osqt)

`osqt` is a Golang library used for the extraction and structured representation of OSQuery tables. It does this by implementing a Golang interpreter capable of understanding the semantics of the DSL used by the OSQuery project to define tables. (`specs/` folder).

## Install

```
$ go get github.com/gen0cide/osqt
```

If you wish to have the CLI:

```
$ go get github.com/gen0cide/osqt/cmd/osqt-cli
```

## Usage

Check Godoc for library information.

## Example

Given the OSQuery Table Definition:

```python
table_name("groups")
description("Local system groups.")
schema([
    Column("gid", BIGINT, "Unsigned int64 group ID", index=True),
    Column("gid_signed", BIGINT, "A signed int64 version of gid"),
    Column("groupname", TEXT, "Canonical local group name"),
])
implementation("groups@genGroups")
```

This library will produce:

```json
{
  "groups": {
    "name": "groups",
    "description": "Local system groups.",
    "schema": [
      {
        "index": 0,
        "name": "gid",
        "type": "BIGINT",
        "description": "Unsigned int64 group ID",
        "options": {
          "index": true
        }
      },
      {
        "index": 1,
        "name": "gid_signed",
        "type": "BIGINT",
        "description": "A signed int64 version of gid"
      },
      {
        "index": 2,
        "name": "groupname",
        "type": "TEXT",
        "description": "Canonical local group name"
      }
    ],
    "implementation": "groups@genGroups"
  }
}
```

## Shoutouts

- davehughes
- javuto
- ahhh
