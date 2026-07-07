Fixed several bugs in how converting an `AggregatedHistogram` into a DDSketch (used by the `datadog_metrics` sink, among others) reported `sum`/`avg`:

- Bucket-boundary interpolation discarded the histogram's already-exact `sum`/`count` and rebuilt approximate versions purely from interpolation. This broke down badly for buckets whose true values sit far from their edges, most notably the unbounded first/last bucket, where interpolation collapses to a point mass at a single finite edge — in some cases inflating the reported `avg`/`sum` by over 1000%. `sum`/`avg` are now taken directly from the source histogram's exact running totals instead.
- Sources that hand us fewer buckets than the histogram's true count (Prometheus always drops its cumulative `+Inf` bucket once converted to deltas) previously caused `avg` to be computed using the smaller, bucket-derived count instead of the histogram's true count, inflating `avg`.
- OTLP histograms that legitimately omit `sum` were defaulted to an exact `0.0`, which is indistinguishable from a genuinely all-zero histogram and was treated as authoritative, corrupting the `sum`/`avg` of otherwise-healthy non-empty histograms. Unknown sums are now represented distinctly so the bucket-derived estimate is used instead of a fabricated zero.

Note that quantile/percentile estimates are unaffected by these fixes, since they are derived from the sketch's bins rather than from `sum`/`count`.

authors: vladimir-dd gwenaskell
