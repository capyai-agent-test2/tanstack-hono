# Five-lane incremental review context fixture

This PR intentionally introduces several contract files in the first commit. A later commit mutates those same-PR contracts while also mutating endpoints that already existed on master.

Expected incremental review behavior: use PR-base context to avoid treating same-PR-only contract edits as backward-incompatible, while still flagging breaks to pre-existing API surfaces.
