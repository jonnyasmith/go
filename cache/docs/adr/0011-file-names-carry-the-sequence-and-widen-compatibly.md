# File names carry the sequence, and the width may widen but never mean two things

Segments and snapshots are named for a sequence — a segment for the first record it holds, a snapshot for the lowest sequence it covers — so recovery can order a directory and prune a covered prefix without opening a single file. The name is zero-padded to a fixed width so lexical order equals sequence order. Earlier binaries wrote an eight-digit stem, which cannot represent the whole `uint64` range; current binaries write twenty.

## Consequences

- Recovery accepts both widths and orders by the parsed number, so a store written by an older binary opens without a migration step. Nothing is renamed in place: files reach the current width by being rolled or superseded, which happens under the store's own lock and needs no separate tool.
- Two names of different widths denoting the same sequence are rejected rather than deduplicated. The store cannot know which file is authoritative, and guessing means silently discarding records.
- The rule lives in the name rather than the file, so the format version cannot express it. That is the cost of putting ordering information in the directory listing, and it is worth paying: the alternative is opening every file to sort them.
- Only these two widths are legal, and a segment may not be named zero. A snapshot may, because a snapshot covering nothing from sequence zero is how an empty store's image is named.
- Inside a file, the version is exact rather than a floor. A header whose version is anything other than the current one refuses the open, as does a nonzero reserved-flags word, because tolerating an unknown value is how a store starts misreading a format it does not implement. A future version will be introduced by teaching the reader that version, not by relaxing this check.
