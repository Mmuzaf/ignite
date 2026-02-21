
# Mmuzaf:ignite — TODO/FIXME Analysis

> This report scans the full git commit history of **Mmuzaf:ignite** (2014–2026) and tracks every line containing a `TODO` or `FIXME` comment (case-insensitive) that was **added** or **removed** in each commit. Merge commits are excluded to avoid double-counting.
>
> For each commit, the diff is parsed line by line: lines prefixed with `+` are additions, lines prefixed with `-` are removals. Results are aggregated by the commit year.
>
> **5,031** TODO/FIXME lines added · **4,514** removed · **517** unresolved backlog across **13** years.

## Combined: Additions and Removals by Year

Each year shows two adjacent bars: additions (green) and removals (red).

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#22aa44,#dd4444'}}, 'xyChart': {'width': 1200, 'height': 500}}}%%
xychart-beta
    title "Mmuzaf:ignite — TODO/FIXME Additions (green) and Removals (red) by Year"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 2200
    bar [2186, 888, 563, 385, 401, 191, 216, 37, 65, 15, 4, 61, 19]
    bar [1685, 1047, 372, 349, 276, 158, 207, 35, 177, 127, 41, 31, 9]
```

## Additions by Year

Number of lines containing `TODO` or `FIXME` added per year. Peaks indicate periods of rapid feature development or technical debt introduction.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#22aa44'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — TODO/FIXME Additions by Year"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 2200
    bar [2186, 888, 563, 385, 401, 191, 216, 37, 65, 15, 4, 61, 19]
```

## Removals by Year

Number of lines containing `TODO` or `FIXME` removed per year. Peaks reflect cleanup efforts or resolution of previously noted issues.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#dd4444'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — TODO/FIXME Removals by Year"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 1700
    bar [1685, 1047, 372, 349, 276, 158, 207, 35, 177, 127, 41, 31, 9]
```

## Cumulative Debt over Time

Running total of unresolved `TODO`/`FIXME` items (additions − removals, accumulated year by year). A rising line means debt is growing; a falling line means active cleanup is outpacing new additions.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — Cumulative TODO/FIXME Debt over Time"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Accumulated Lines" 0 --> 740
    line [501, 342, 533, 569, 694, 727, 736, 738, 626, 514, 477, 507, 517]
```

## Net Change per Year

Difference between additions and removals for each year (additions − removals). Positive values mean more TODOs were introduced than resolved; negative values indicate a net cleanup year.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — TODO/FIXME Net Change per Year"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Net Lines" -160 --> 510
    line [501, -159, 191, 36, 125, 33, 9, 2, -112, -112, -37, 30, 10]
```

## Cleanup Ratio per Year

Percentage of new `TODO`/`FIXME` lines that were also removed in the same year (removals / additions × 100). 100% = breakeven; above 100% = paying down old debt; well below 100% = accumulating backlog.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — TODO/FIXME Cleanup Ratio per Year (Removals / Additions %)"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "%" 0 --> 1100
    line [77, 118, 66, 91, 69, 83, 96, 95, 272, 847, 1025, 51, 47]
```

## Cumulative Additions vs Removals

Total ever-added (green) and total ever-removed (red) `TODO`/`FIXME` lines over the project lifetime. The widening gap between the two lines represents the current unresolved backlog.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#22aa44,#dd4444'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — Cumulative TODO/FIXME Additions vs Removals"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 5100
    line [2186, 3074, 3637, 4022, 4423, 4614, 4830, 4867, 4932, 4947, 4951, 5012, 5031]
    line [1685, 2732, 3104, 3453, 3729, 3887, 4094, 4129, 4306, 4433, 4474, 4505, 4514]
```

## TODO/FIXME Introduction Rate (per kLOC added)

For every 1,000 lines of new code written in a given year, how many `TODO`/`FIXME` comments were introduced. Measures developer discipline at the time of writing, independent of total codebase size. A spike means the team was moving fast and leaving more markers behind; a falling trend means code quality is improving.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — TODO/FIXME Introduction Rate (per kLOC added)"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "TODOs / kLOC" 0 --> 2
    line [1.18, 0.84, 0.86, 0.54, 0.46, 0.39, 0.46, 0.18, 0.08, 0.11, 0.05, 0.45, 0.85]
```

## Running TODO/FIXME Debt Density (per kLOC)

The current density of unresolved `TODO`/`FIXME` items per 1,000 lines of code in the codebase, tracked at the end of each year. Unlike raw counts, this normalises for codebase growth — a flat or falling line means the codebase is getting cleaner even if absolute TODO counts rise. An upward trend is a red flag: debt is growing faster than the code.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:ignite — Running TODO/FIXME Debt Density (per kLOC)"
    x-axis [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "TODOs / kLOC" 0 --> 1
    line [0.5, 0.24, 0.29, 0.25, 0.24, 0.23, 0.24, 0.23, 0.18, 0.16, 0.15, 0.16, 0.16]
```

