# Data quality and interpretation notes

## Known limitations
- Some ward-level records are estimates mapped from older BBMP 198-ward geography to the DataMeet 243-ward geography.
- Pothole complaint data used in the current analysis is from 2022.
- Ward-level spending covers 2018–2023 and uses estimated ward mapping.
- Work-order financial values can repeat across ward-allocation rows. Aggregations are preliminary until transaction uniqueness and allocation rules are validated.
- Missing contractor names are grouped under `Unknown` in the relevant analysis.
- Some source fields are null. If nulls are converted to zero for dashboard KPIs, document that as a reporting assumption.
- The source refresh cadence is not confirmed; do not describe the data as real-time.

## Before publishing financial metrics
1. Confirm the grain of each source record.
2. Determine whether repeated work-order values represent allocation rows or separate transactions.
3. Validate overlap-share logic and missing amounts.
4. Compare totals with a trusted source-level total if available.
5. Label unresolved totals as preliminary.

## Suggested dashboard disclosure
> Pothole complaint data is from 2022. Ward-level spending for 2018–2023 uses estimated ward mappings. Contractor financial aggregates are preliminary because work-order values may repeat across ward-allocation records. Missing contractor names are grouped under “Unknown.” Null-to-zero assumptions may affect selected KPIs.
