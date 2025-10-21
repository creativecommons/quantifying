# Standard library
import os
import sys

# Third-party
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import colormaps

# Add parent directory so shared can be imported
sys.path.append(os.path.dirname(__file__))

# First-party/Local
import shared  # noqa: E402


def annotate_ylabels(ax, data, data_label, colors):
    i = 0
    c = 0
    ytick = ax.yaxis.get_major_ticks(numticks=1)[0]
    #    defaults: ytick.major.size         + ytick.major.pad
    indent = -1 * (ytick.get_tick_padding() + ytick.get_pad())
    for index, row in data.iterrows():
        if c >= len(colors):
            c = 0

        # annotate totals
        ax.annotate(
            f"    {row[data_label]:>15,d}",
            (indent, i - 0.1),
            xycoords=("axes points", "data"),
            color=colors[c],
            fontsize="x-small",
            horizontalalignment="right",
            verticalalignment="top",
        )

        # annotate percentages
        percent = row[data_label] / data[data_label].sum() * 100
        if percent < 0.1:
            percent = "< .1%"
        else:
            percent = f"{percent:4.1f}%"
        ax.annotate(
            percent,
            (1.02, i),
            xycoords=("axes fraction", "data"),
            backgroundcolor=colors[c],
            color="white",
            fontsize="x-small",
            horizontalalignment="left",
            verticalalignment="center",
        )

        i += 1
        c += 1
    return ax


def combined_plot(
    args, data, title, name_label, data_label, bar_xscale=None, bar_ylabel=None
):
    if len(data) > 10:
        raise shared.QuantifyingException(
            "the combined_plot() function is limited to a maximum of 10 data"
            " points"
        )

    plt.rcParams.update({"font.family": "monospace", "figure.dpi": 300})

    height = 1 + len(data) * 0.5
    if height < 2.5:
        height = 2.5

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(8, height), width_ratios=(2, 1), layout="constrained"
    )
    colors = colormaps["tab10"].colors

    # 1st axes: horizontal barplot of counts
    # pad tick labels to make room for annotation
    tick_labels = []
    for index, row in data.iterrows():
        count = f"{row[data_label]:,d}"
        tick_labels.append(f"{index}\n{' ' * len(count)}")
    if bar_xscale == "log":
        log = True
    else:
        bar_xscale = "linear"
        log = False
    ax1.barh(y=tick_labels, width=data[data_label], color=colors, log=log)
    ax1.tick_params(axis="x", which="major", labelrotation=45)
    ax1.set_xlabel("Number of works")
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(number_formatter))
    if bar_ylabel is not None:
        ax1.set_ylabel(bar_ylabel)
    else:
        ax1.set_ylabel(name_label)
    ax1 = annotate_ylabels(ax1, data, data_label, colors)

    # 2nd axes: pie chart of percentages
    data.plot.pie(
        ax=ax2,
        y=data_label,
        colors=colors,
        labels=None,
        legend=False,
        radius=1.25,
    )
    ax2.set_title("Percent")
    ax2.set_ylabel(None)

    # plot
    plt.suptitle(title)
    plt.annotate(
        f"Creative Commons (CC)\nbar x scale: {bar_xscale}, data from"
        f" {args.quarter}",
        (0.95, 5),
        xycoords=("figure fraction", "figure points"),
        color="gray",
        fontsize="x-small",
        horizontalalignment="right",
    )

    if args.show_plots:
        plt.show()

    return plt


def number_formatter(x, pos):
    """
    Use the millions formatter for x-axis

    The two args are the value (x) and tick position (pos)
    """
    if x >= 1e9:
        return f"{x * 1e-9:,.0f}B"
    elif x >= 1e6:
        return f"{x * 1e-6:,.0f}M"
    elif x >= 1e3:
        return f"{x * 1e-3:,.0f}K"
    else:
        return f"{x:,.0f}"
