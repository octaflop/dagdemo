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
    mo.stop(not materialize_button.value)
    name = asset_dropdown.value
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
    mo.stop(success is None)
    mo.md(f"## `{name}`")
    if not success:
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
def _(datetime, mo, name, output, success):
    mo.stop(not success)
    mo.stop(not isinstance(output, list) or len(output) == 0)

    mo.md("---")

    if name == "ut_legislators":
        _legislator_viz(mo, output, bar_chart)
    elif name == "ut_bills":
        _bills_viz(mo, output, bar_chart)
    elif name in ("sltrib_articles", "sltrib_full_content", "ai_enriched_articles"):
        _articles_viz(mo, output, name, bar_chart, datetime)
    return


if __name__ == "__main__":
    app.run()
