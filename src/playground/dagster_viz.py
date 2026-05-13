import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from datetime import datetime, timezone

    return datetime, mo


@app.cell
def _():
    from dagster import materialize, AssetSelection
    from dagdemo import defs as dagdemo_defs

    return AssetSelection, dagdemo_defs, materialize


@app.function
def bar_chart(counts: dict, title: str = "") -> list[str]:
    if not counts:
        return ["*No data*"]
    max_val = max(counts.values())
    max_label = max(len(str(k)) for k in counts)
    lines = [f"**{title}**", ""] if title else []
    for label, val in sorted(counts.items(), key=lambda x: -x[1]):
        bar = "█" * max(1, int(40 * val / max_val))
        lines.append(f"`{str(label):<{max_label}}` {bar} **{val}**")
    return lines


@app.function
def _legislator_viz(mo, output, bar_chart_fn):
    from collections import Counter

    parties = Counter(leg.get("party", "Unknown") for leg in output)
    houses = Counter(leg.get("house", "Unknown") for leg in output)
    districts = Counter(leg.get("district", "N/A") for leg in output if leg.get("district") is not None)

    mo.vstack([
        mo.hstack([
            mo.vstack([
                mo.md("### Party Distribution"),
                mo.md("\n".join(bar_chart_fn(dict(parties), "Parties"))),
            ]),
            mo.vstack([
                mo.md("### Chamber Distribution"),
                mo.md("\n".join(bar_chart_fn(dict(houses), "Chamber"))),
            ]),
            mo.vstack([
                mo.md("### Top Districts"),
                mo.md("\n".join(bar_chart_fn(dict(districts.most_common(10)), "District"))),
            ]),
        ]),
        mo.md("### Legislators"),
        mo.ui.table(output, page_size=10, selection=None),
    ])


@app.function
def _bills_viz(mo, output, bar_chart_fn):
    from collections import Counter

    statuses = Counter(b.get("status", "Unknown") for b in output)
    sponsors = Counter(b.get("sponsor", "Unknown") for b in output if b.get("sponsor"))
    top_sponsors = dict(sponsors.most_common(10))

    recent_bills = sorted(
        [b for b in output if b.get("last_action_date")],
        key=lambda x: str(x.get("last_action_date")),
        reverse=True,
    )[:20]

    mo.vstack([
        mo.hstack([
            mo.vstack([
                mo.md("### Bill Status"),
                mo.md("\n".join(bar_chart_fn(dict(statuses), "Status"))),
            ]),
            mo.vstack([
                mo.md("### Top Sponsors"),
                mo.md("\n".join(bar_chart_fn(top_sponsors, "Sponsor"))),
            ]),
        ]),
        mo.md("### Recent Bills"),
        mo.ui.table(recent_bills, page_size=10, selection=None),
    ])


@app.function
def _articles_viz(mo, output, name, bar_chart_fn, datetime):
    from collections import Counter

    sources = Counter(a.get("source", "unknown") for a in output)
    authors = Counter(a.get("author") or "Unknown" for a in output if a.get("author"))
    top_authors = dict(authors.most_common(10))

    content_lengths = [len(a.get("full_content") or a.get("content") or "") for a in output]
    avg_length = sum(content_lengths) / len(content_lengths) if content_lengths else 0

    # Build a mini timeline of articles by day
    dates = Counter()
    for a in output:
        pd = a.get("pub_date")
        if pd:
            try:
                if isinstance(pd, str):
                    dt = datetime.fromisoformat(pd.replace("Z", "+00:00"))
                else:
                    dt = pd
                dates[dt.strftime("%Y-%m-%d")] += 1
            except Exception:
                pass

    if name == "ai_enriched_articles":
        categories = Counter()
        sentiments = Counter()
        tones = Counter()
        for a in output:
            analysis = a.get("ai_enrichment", {}).get("analysis", {})
            for cat in analysis.get("categories", []):
                categories[cat] += 1
            sentiments[analysis.get("sentiment", "unknown")] += 1
            tones[analysis.get("tone", "unknown")] += 1

        ai_section = mo.vstack([
            mo.hstack([
                mo.vstack([
                    mo.md("### Categories"),
                    mo.md("\n".join(bar_chart_fn(dict(categories), "Categories"))),
                ]),
                mo.vstack([
                    mo.md("### Sentiment"),
                    mo.md("\n".join(bar_chart_fn(dict(sentiments), "Sentiment"))),
                ]),
                mo.vstack([
                    mo.md("### Tone"),
                    mo.md("\n".join(bar_chart_fn(dict(tones), "Tone"))),
                ]),
            ]),
        ])
    else:
        ai_section = mo.md("")

    timeline = dict(dates.most_common(14))

    mo.vstack([
        mo.hstack([
            mo.vstack([
                mo.md("### Sources"),
                mo.md("\n".join(bar_chart_fn(dict(sources), "Source"))),
            ]),
            mo.vstack([
                mo.md("### Top Authors"),
                mo.md("\n".join(bar_chart_fn(top_authors, "Author"))),
            ]),
        ]),
        ai_section,
        mo.hstack([
            mo.vstack([
                mo.md(f"### Publishing Timeline ({len(timeline)} days)"),
                mo.md("\n".join(bar_chart_fn(timeline, "Date"))),
            ]),
            mo.vstack([
                mo.md("### Content Stats"),
                mo.md(f"**Average content length:** {avg_length:,.0f} chars"),
                mo.md(f"**Total articles:** {len(output)}"),
                mo.md(f"**With images:** {sum(1 for a in output if a.get('thumbnail_url'))}"),
            ]),
        ]),
        mo.md("### Articles Preview"),
        mo.ui.table(output[:20], page_size=10, selection=None),
    ])


@app.cell
def _(dagdemo_defs):
    dagdemo_defs.resolve_asset_graph().asset_nodes
    return


@app.cell
def _(dagdemo_defs, mo):
    assets = sorted(
        dagdemo_defs.resolve_asset_graph().asset_nodes, key=lambda a: a.key.to_user_string()
    )
    asset_names = sorted([a.key.to_user_string() for a in assets])
    group_names = sorted({a.group_name for a in assets if a.group_name})

    asset_dropdown = mo.ui.dropdown(
        options=asset_names,
        label="Asset",
        full_width=True,
    )
    materialize_button = mo.ui.button(label="Materialize", kind="success")

    mo.hstack(
        [
            mo.md("## Dagster Assets  " + " ".join(f"`{g}`" for g in group_names)),
            mo.hstack([asset_dropdown, materialize_button], justify="start"),
        ],
        justify="space-between",
        gap=2,
    )
    return asset_dropdown, materialize_button


@app.cell
def _(
    AssetSelection,
    asset_dropdown,
    dagdemo_defs,
    materialize,
    materialize_button,
    mo,
):
    name = asset_dropdown.value
    output = None
    success = None
    if materialize_button.value and name:
        with mo.status.spinner(title=f"Materializing **{name}** ..."):
            result = materialize(
                [dagdemo_defs],
                selection=AssetSelection.keys(name).upstream(),
            )
        success = result.success
        output = result.output_for_node(name) if success else None
    return name, output, success


@app.cell
def _(mo, name, output, success):
    if success is None:
        mo.callout(
            "Select an asset and click **Materialize** to preview output.",
            kind="info",
        )
    elif not success:
        mo.callout("Materialization failed", kind="danger")
    elif output is None:
        mo.callout(f"**{name}** succeeded but produced no output", kind="warn")
    elif isinstance(output, str) and output.strip().startswith("<"):
        mo.vstack([
            mo.md(f"### Raw XML — {len(output):,} bytes"),
            mo.ui.code_editor(
                value=output[:5000],
                language="xml",
                disabled=True,
                min_height=8,
                max_height=25,
            ),
        ])
    elif isinstance(output, str):
        mo.vstack([
            mo.md(f"### Raw text — {len(output):,} chars"),
            mo.ui.code_editor(
                value=output[:5000],
                language="text",
                disabled=True,
                max_height=25,
            ),
        ])
    elif isinstance(output, list):
        count = len(output)
        mo.md(f"### {count} records")
        if count > 0:
            mo.ui.table(output, page_size=10, selection=None)
    else:
        mo.ui.code_editor(
            value=repr(output)[:5000],
            language="python",
            disabled=True,
            max_height=20,
        )
    return


@app.cell
def _(datetime, mo, name, output, success, bar_chart):
    if success and isinstance(output, list) and len(output) > 0:
        if name == "ut_legislators":
            _legislator_viz(mo, output, bar_chart)
        elif name == "ut_bills":
            _bills_viz(mo, output, bar_chart)
        elif name in ("sltrib_articles", "sltrib_full_content", "ai_enriched_articles"):
            _articles_viz(mo, output, name, bar_chart, datetime)
    return


if __name__ == "__main__":
    app.run()
