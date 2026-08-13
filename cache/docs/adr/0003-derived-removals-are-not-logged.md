# Expiry and eviction write nothing to the log

Both remove an entry, and neither writes a record. Deadlines are already durable in the record that created the entry, so recovery can re-evaluate expiry itself and remains deterministic without help; logging expiries would double the log for a store of short-lived keys that no caller is writing to. Eviction is a decision about memory, not about data, and the log describes data.

## Consequences

- An entry evicted while still live **reappears on recovery**, because nothing durable ever said it left. This is intended: eviction reflects the memory pressure of one process lifetime, not a change to the store's contents.
- Recovery therefore restores a store that may immediately exceed capacity, and evicts down to the bound as part of opening.
- A reader can never observe a resurrected entry that should have expired, since expiry is evaluated against the deadline on every read.
