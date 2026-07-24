# Pinned GeCo source

`geco-data-generator-corruptor.tar.gz` is the unmodified archive downloaded
from the [official Australian National University GeCo page][page] on
2026-07-22.

- Direct source URL: `https://dmm.anu.edu.au/geco/geco-data-generator-corruptor.tar.gz`
- SHA-256: `676b143d208b44a306002c05fb2b17d1a21c40e1972d4be7db8b03c130d0ab0c`
- Upstream license: Mozilla Public License 2.0 (`MPL2.0.txt` inside the archive)
- Upstream language: Python 2

E11 verifies the checksum, extracts safely into a temporary directory, runs a
mechanical `lib2to3` conversion over the five source modules, compiles them,
and preserves the exact unified diff with the experiment results. No generator
or corruption logic is replaced by the adapter.

[page]: https://dmm.anu.edu.au/geco/
