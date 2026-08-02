# LOB factor review and next replication

## Evaluation convention

Use the registered `lob-domain-neutralization` protocol. Treat the nine pre-specified domains as primary, then report a pooled domain-neutral aggregate, an unpartitioned neutralized market result, and raw market diagnostics. Within domains neutralize on `non_linear_size`, `momentum`, `liquidity`, `beta`, and `residual_volatility`; exclude linear `size`.

## Factors to extend

| Priority | Factor | Frequency and horizon | Decision |
|---|---|---|---|
| A | old active mid-gap absolute strength | intraday 10:00–10:30; 5/10/15/30 minutes | Extend. The 15-minute LOB5-ex-size D10–D1 is about 12.10 bp with t=3.61. Treat as a tail signal, not a linear rank factor. |
| A | old active mid-gap ratio | intraday 10:00–10:30; 10/15 minutes | Extend. Style explanation is moderate and neutralized tail spreads remain around 4 bp with t slightly above 2. |
| A- | strict active D01 reversal | daily 10:00–close to next-day open-to-open | Extend by domain. The 50–500亿元, non-STAR, price-below-10 domain has IC about 3.47% with t=3.04, but spread evidence is weaker. |
| B | all-TRADE D01 reversal | intraday 10:00–10:30; especially 15 minutes | Extend as a robustness companion. Full-market LOB5-ex-size IC is weak, while the 15-minute spread is about 3.04 bp with t=2.16. |
| B | D03 historical extreme event at 90/95% | daily and intraday event study | Extend only as an event factor. Compute the primitive once and retain both thresholds; prioritize event-minus-control returns and reversal horizons. |
| Diagnostic | old signed active mid-gap | intraday | Save when produced in the active-take pass, but do not promote. It has low style explanation and weak neutralized performance. |
| Drop | D02 variants | all | Do not calculate independently when D02 is exactly negative D01; derive it mechanically if needed. |
| Drop | daily old active absolute/ratio and daily all-TRADE D01 | daily | Do not prioritize. Current open-to-open performance is weak or style-mediated. |

## Efficient replication groups

1. Active-take pass: produce old absolute strength, ratio, signed diagnostic, strict D01, event counts, and shared primitives from one LOB read.
2. All-TRADE/order pass: produce all-TRADE D01 and D03 primitives plus both historical thresholds from one LOB read.

## Microstructure observations

1. Absolute active mid-gap is strongly state- and style-dependent. High values concentrate in high-beta, high-momentum, liquid, volatile names; it is partly an activity-state proxy.
2. The surviving signal is nonlinear. Neutralization reduces broad Rank IC much more than extreme-decile spreads, indicating that the useful information is concentrated in unusually intense events.
3. Predictability builds over roughly 10–15 minutes rather than appearing entirely in the first five minutes, then loses statistical strength over longer horizons. This is consistent with gradual price-pressure digestion followed by book replenishment.
4. Unsigned intensity outperforms signed direction. The magnitude of aggressive interaction appears more informative than its classified buy/sell sign, which may be noisy or quickly offset by liquidity provision.
5. Domain behavior is structural, not merely instability. Tick size, depth, participant mix, and resiliency differ across capitalization, price, and board regimes; domain signs need not agree.
6. Size bucketing removes much of the between-domain scale effect. Excluding linear size from within-domain neutralization changes key domain results only modestly, while all-market results remain size-sensitive.
7. D03 is better viewed as a pressure-and-reversal event. Its sparse distribution and sign changes across horizons make ordinary continuous IC an incomplete description.

All conclusions are based on only 20 trading days in January 2026 and require a pre-specified longer sample before promotion to a production factor.
