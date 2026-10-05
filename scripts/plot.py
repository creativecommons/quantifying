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
            f"    {int(row[data_label]):>15,d}",
            (indent, i - 0.22),
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
        1, 2, figsize=(10, height), width_ratios=(2, 1), layout="constrained"
    )
    colors = colormaps["tab10"].colors

    # 1st axes: horizontal barplot of counts
    # pad tick labels to make room for annotation
    tick_labels = []
    for index, row in data.iterrows():
        count = f"{int(row[data_label]):,d}"
        tick_labels.append(f"{wrap_label(index)}\n{' ' * len(count)}")
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


def wrap_label(label):
    if " " not in label:
        return label

    midpoint = len(label) // 2
    # find nearest space to midpoint
    left = label.rfind(" ", 0, midpoint)
    right = label.find(" ", midpoint)

    if left == -1:
        split_index = right
    elif right == -1:
        split_index = left
    else:
        if midpoint - left <= right - midpoint:
            split_index = left
        else:
            split_index = right
    if split_index == -1:
        return label

    return f"{label[:split_index]}\n{label[split_index + 1:]}"


def stacked_barh_plot(
    args,
    data,
    title,
    name_label,
    stack_labels,
    xscale="linear",
    ylabel=None,
):
    """
    Create a stacked horizontal bar plot.
    """
    if len(data) > 10:
        raise shared.QuantifyingException(
            "stacked_barh_plot() is limited to a maximum of 10 data points"
        )

    plt.rcParams.update({"font.family": "monospace", "figure.dpi": 300})

    height = max(2.5, 1 + len(data) * 0.5)
    fig, ax = plt.subplots(figsize=(10, height), layout="constrained")

    colors = colormaps["tab10"].colors
    left = [0] * len(data)

    # stacked bars
    for i, label in enumerate(stack_labels):
        ax.barh(
            y=data.index,
            width=data[label],
            left=left,
            color=colors[i % len(colors)],
            label=label,
            log=(xscale == "log"),
        )
        left = [
            current_left + width
            for current_left, width in zip(left, data[label])
        ]

    ax.set_xlabel("Percentage of works")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(number_formatter))
    ax.set_yticks(range(len(data.index)))
    ax.set_yticklabels([wrap_label(label) for label in data.index])

    if ylabel:
        ax.set_ylabel(ylabel)
    else:
        ax.set_ylabel(name_label)

    ax.legend(
        title="Type",
        fontsize="x-small",
        title_fontsize="x-small",
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
    )

    plt.suptitle(title)
    plt.annotate(
        f"Creative Commons (CC)\nbar x scale: {xscale}, data from"
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
