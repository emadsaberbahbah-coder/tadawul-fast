The source provider can return a blank Name for an unpriced instrument. A later
sync then clears a reviewed issuer caption from the workbook. The final RAW
writer now fills that blank from `data/verified_name_labels.json` after restoration
and identity guards and before whole-page ranking and copy-only presentation.

The reviewed registry contains 39 fund captions and 16 global-market captions.
Each entry exposes its exact raw Symbol, page, unchanged native metadata fence,
legal-name caption, security-class identifiers, bounded listing-state statement,
public authority URLs, raw response SHA256 digests, capture times and review
receipt digest. The saved public authority bodies were independently rehashed
during registry preparation. Private workbook exports, account observations,
broker records and local source paths are absent from this registry.

The review digest is reproducible from the public binding: UTF-8 JSON with
sorted keys, compact separators and Unicode preserved, excluding only the
`review_receipt_sha256` field itself. Original authority-review times govern
expiry; rebuilding or loading the map does not renew a review.

Matching never trims, case-folds, aliases or replaces a Symbol. The current Name
must be blank, the price must be missing or finite and nonpositive, and the row
must retain an explicit blocked/low-trust/quarantined state. Positive, malformed
or nonfinite prices are held. Existing Names are never overwritten. Exact class,
exchange, currency and country are required; missing metadata is held. The two
explicit all-blank metadata bindings require the actual `identity_quarantined:`
producer token, with only `None` and empty strings accepted as absent metadata.
They provide an issuer caption while keeping the entire quarantine intact.

Malformed maps, duplicate identities, unavailable fields, ambiguous rows and
unrecognized action/status values are held. Reviews expire after seven days;
expiry prevents filling a newly blank Name, and never deletes an existing Name.
Renewal requires another current authority review and a reviewed registry change.
No arbitrary old workbook Name is carried forward.

The pure helper changes only Name. Prices, targets, returns, financial models,
warnings, scores, ranks, membership, portfolio holdings and eligibility remain
unchanged. Closed or suspended names are captions for the exact identified
instrument; they do not certify an active listing, current tradability, a new
private security, a successor identity or custody.

After a successful RAW update, the producer receipt reports the exact registry
digest, applied/held/skipped counts and reasons, and the outgoing payload digest.
It explicitly declares `live_readback: false`. This receipt is not an independent
Sheet readback, native-script installation witness or end-to-end consumption proof.
The focused suite is required in the blocking lean CI lane.
