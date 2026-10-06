# ZIP source bodies

Parser version 6 reads root ZIP responses whose bodies begin with a ZIP signature.
Stored and DEFLATE members are read in memory using Python's documented
[zipfile API](https://docs.python.org/3.14/library/zipfile.html#zipfile.ZipFile.open).
No member is extracted to disk or executed, and no password is requested.

The supported UTF-8 text member extensions are `.txt`, `.json`, `.yaml`, `.yml`,
`.csv`, and `.xml`. Each complete member goes through the existing format parsers
with the source's protocol hint. Credentials, TLS, transport and unknown native
connection options therefore retain their existing representation. DNS, routing,
listeners and inbounds retain the existing parser's source roles. ZIP filenames
do not supply a protocol hint. Every archived record must declare its protocol or
have one unambiguous source hint. Bare address/port records without a protocol are
reported as `missing_zip_protocol`, preventing relay IP pools from becoming
complete proxy records. Existing standalone plaintext behavior is unchanged.

Directory entries, special files, hidden metadata, unsafe paths, documentation,
example/sample directories, executable source files and unsupported extensions
are skipped. Native WireGuard `.conf` profiles need a separate format batch.
Nested archives, encrypted members, other compression methods and binary members
are unsupported. A malformed ZIP body is never scanned as plaintext. A member
with a failed CRC or invalid encoding contributes no partial records; other valid
members remain eligible.

An archive may have at most 1,024 entries. Each eligible member may expand to
16 MiB, and the cumulative eligible member sizes may total at most 64 MiB.
Reads are bounded and size/CRC checked before parsing. Limit and damaged-member
warnings are available in the ordinary parser summary.

The confirmed cached sources include `pojiezhiyuanjun/freev2` archives with native
Clash YAML and URI-list members, and ConfigStream archives with `proxies.txt`.
The audit compares complete connection keys against independently decoded members
parsed by version 5. Large manifests and source bodies stay in the isolated server
workspace; the extractor report names only audited replay URLs.

Identical connection keys deduplicate across members and source URLs. YAML and URI
representations of the same configuration can have different existing settings
representations, so extraction totals and connection-key counts are not unique
configuration growth or database import counts. The publication runner measures
imports separately.
