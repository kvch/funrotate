# funrotate

The project is inspired by `logrotate`. It supports similar configuration
options, but usually with a twist. DO NOT USE THE TOOL.

Quirks:

* no matter what rotation strategy you choose, the tool will always use copytruncate
* you can only disable copytruncate, if you select nocopytruncate
* instead of compressing files, it duplicates every byte in every line
* "compressed" files are named {filename}.zip

## Configuration

You can configure `funrotate` using the file `funrotate.toml`.

```toml
[[files]]
path = "file.log"
interval = "daily"
max_files = 3
compress = true
size = 1024
strategy = "create" # copytruncate

[[files]]
path = "other-file.log"
interval = "daily"
max_files = 4
compress = false
size = 1024
strategy = "copytruncate" # copytruncate
```
