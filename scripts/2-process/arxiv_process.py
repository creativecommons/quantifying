#!/usr/bin/env python
"""
Process ArXiv data.
"""
# Standard library
import argparse
import os
import sys
import textwrap
import traceback

# Third-party
import pandas as pd
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])

# ArXiv Categories mapping for readable names
CATEGORIES = {
    # Computer Science
    "cs.AI": "Artificial Intelligence",
    "cs.AR": "Hardware Architecture",
    "cs.CC": "Computational Complexity",
    "cs.CE": "Computational Engineering, Finance, and Science",
    "cs.CG": "Computational Geometry",
    "cs.CL": "Computation and Language",
    "cs.CR": "Cryptography and Security",
    "cs.CV": "Computer Vision and Pattern Recognition",
    "cs.CY": "Computers and Society",
    "cs.DB": "Databases",
    "cs.DC": "Distributed, Parallel, and Cluster Computing",
    "cs.DL": "Digital Libraries",
    "cs.DM": "Discrete Mathematics",
    "cs.DS": "Data Structures and Algorithms",
    "cs.ET": "Emerging Technologies",
    "cs.FL": "Formal Languages and Automata Theory",
    "cs.GL": "General Literature",
    "cs.GR": "Graphics",
    "cs.GT": "Computer Science and Game Theory",
    "cs.HC": "Human-Computer Interaction",
    "cs.IR": "Information Retrieval",
    "cs.IT": "Information Theory",
    "cs.LG": "Machine Learning",
    "cs.LO": "Logic in Computer Science",
    "cs.MA": "Multiagent Systems",
    "cs.MM": "Multimedia",
    "cs.MS": "Mathematical Software",
    "cs.NA": "Numerical Analysis",
    "cs.NE": "Neural and Evolutionary Computing",
    "cs.NI": "Networking and Internet Architecture",
    "cs.OH": "Other Computer Science",
    "cs.OS": "Operating Systems",
    "cs.PF": "Performance",
    "cs.PL": "Programming Languages",
    "cs.RO": "Robotics",
    "cs.SC": "Symbolic Computation",
    "cs.SD": "Sound",
    "cs.SE": "Software Engineering",
    "cs.SI": "Social and Information Networks",
    "cs.SY": "Systems and Control",
    # Mathematics
    "math.AC": "Commutative Algebra",
    "math.AG": "Algebraic Geometry",
    "math.AP": "Analysis of PDEs",
    "math.AT": "Algebraic Topology",
    "math.CA": "Classical Analysis and ODEs",
    "math.CO": "Combinatorics",
    "math.CT": "Category Theory",
    "math.CV": "Complex Variables",
    "math.DG": "Differential Geometry",
    "math.DS": "Dynamical Systems",
    "math.FA": "Functional Analysis",
    "math.GM": "General Mathematics",
    "math.GN": "General Topology",
    "math.GR": "Group Theory",
    "math.GT": "Geometric Topology",
    "math.HO": "History and Overview",
    "math.IT": "Information Theory",
    "math.KT": "K-Theory and Homology",
    "math.LO": "Logic",
    "math.MG": "Metric Geometry",
    "math.MP": "Mathematical Physics",
    "math.NA": "Numerical Analysis",
    "math.NT": "Number Theory",
    "math.OA": "Operator Algebras",
    "math.OC": "Optimization and Control",
    "math.PR": "Probability",
    "math.QA": "Quantum Algebra",
    "math.RA": "Rings and Algebras",
    "math.RT": "Representation Theory",
    "math.SG": "Symplectic Geometry",
    "math.SP": "Spectral Theory",
    "math.ST": "Statistics Theory",
    # Physics
    "physics.acc-ph": "Accelerator Physics",
    "physics.ao-ph": "Atmospheric and Oceanic Physics",
    "physics.app-ph": "Applied Physics",
    "physics.atm-clus": "Atomic and Molecular Clusters",
    "physics.atom-ph": "Atomic Physics",
    "physics.bio-ph": "Biological Physics",
    "physics.chem-ph": "Chemical Physics",
    "physics.class-ph": "Classical Physics",
    "physics.comp-ph": "Computational Physics",
    "physics.data-an": "Data Analysis, Statistics and Probability",
    "physics.ed-ph": "Physics Education",
    "physics.flu-dyn": "Fluid Dynamics",
    "physics.gen-ph": "General Physics",
    "physics.geo-ph": "Geophysics",
    "physics.hist-ph": "History and Philosophy of Physics",
    "physics.ins-det": "Instrumentation and Detectors",
    "physics.med-ph": "Medical Physics",
    "physics.optics": "Optics",
    "physics.plasm-ph": "Plasma Physics",
    "physics.pop-ph": "Popular Physics",
    "physics.soc-ph": "Physics and Society",
    "physics.space-ph": "Space Physics",
    # Statistics
    "stat.AP": "Applications",
    "stat.CO": "Computation",
    "stat.ME": "Methodology",
    "stat.ML": "Machine Learning",
    "stat.OT": "Other Statistics",
    "stat.TH": "Statistics Theory",
    # Quantitative Biology
    "q-bio.BM": "Biomolecules",
    "q-bio.CB": "Cell Behavior",
    "q-bio.GN": "Genomics",
    "q-bio.MN": "Molecular Networks",
    "q-bio.NC": "Neurons and Cognition",
    "q-bio.OT": "Other Quantitative Biology",
    "q-bio.PE": "Populations and Evolution",
    "q-bio.QM": "Quantitative Methods",
    "q-bio.SC": "Subcellular Processes",
    "q-bio.TO": "Tissues and Organs",
    # Quantitative Finance
    "q-fin.CP": "Computational Finance",
    "q-fin.EC": "Economics",
    "q-fin.GN": "General Finance",
    "q-fin.MF": "Mathematical Finance",
    "q-fin.PM": "Portfolio Management",
    "q-fin.PR": "Pricing of Securities",
    "q-fin.RM": "Risk Management",
    "q-fin.ST": "Statistical Finance",
    "q-fin.TR": "Trading and Market Microstructure",
    # High Energy Physics
    "hep-ex": "High Energy Physics - Experiment",
    "hep-lat": "High Energy Physics - Lattice",
    "hep-ph": "High Energy Physics - Phenomenology",
    "hep-th": "High Energy Physics - Theory",
    # Nuclear Physics
    "nucl-ex": "Nuclear Experiment",
    "nucl-th": "Nuclear Theory",
    # Quantum Physics
    "quant-ph": "Quantum Physics",
}


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=QUARTER,
        help=f"Data quarter in format YYYYQx (default: {QUARTER})",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results (default: False)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions such as fetch, merge, add, commit, and push"
        " (default: False)",
    )
    return parser.parse_args()


def data_to_csv(args, data, file_path):
    """Save DataFrame to CSV if save is enabled."""
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    if args.enable_save:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        data.to_csv(
            file_path, index=False, quoting=1
        )  # quoting=1 quotes all fields


def process_license_totals(args, count_data):
    """
    Processing count data: totals by license
    """
    LOGGER.info(process_license_totals.__doc__.strip())
    data = count_data.groupby(["TOOL_IDENTIFIER"], as_index=False)[
        "COUNT"
    ].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "TOOL_IDENTIFIER": "License",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_license.csv"
    )
    data_to_csv(args, data, file_path)


def process_category_totals(args, category_data):
    """
    Processing category data: totals by category
    """
    LOGGER.info(process_category_totals.__doc__.strip())
    data = category_data.groupby(["CATEGORY_CODE"], as_index=False)[
        "COUNT"
    ].sum()
    data = data.sort_values("COUNT", ascending=False)
    data.reset_index(drop=True, inplace=True)

    # Add readable category names
    data["Category_Name"] = data["CATEGORY_CODE"].map(CATEGORIES)
    # For categories not in our mapping, use the code as name
    data["Category_Name"] = data["Category_Name"].fillna(data["CATEGORY_CODE"])

    data.rename(
        columns={
            "CATEGORY_CODE": "Category_Code",
            "COUNT": "Count",
        },
        inplace=True,
    )
    # Reorder columns to have Code, Name, Count
    data = data[["Category_Code", "Category_Name", "Count"]]

    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_category.csv"
    )
    data_to_csv(args, data, file_path)


def process_year_totals(args, year_data):
    """
    Processing year data: totals by year
    """
    LOGGER.info(process_year_totals.__doc__.strip())
    data = year_data.groupby(["YEAR"], as_index=False)["COUNT"].sum()
    data = data.sort_values("YEAR", ascending=True)
    data.reset_index(drop=True, inplace=True)
    data.rename(
        columns={
            "YEAR": "Year",
            "COUNT": "Count",
        },
        inplace=True,
    )
    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_year.csv"
    )
    data_to_csv(args, data, file_path)


def process_author_bucket_totals(args, author_data):
    """
    Processing author bucket data: totals by author bucket
    """
    LOGGER.info(process_author_bucket_totals.__doc__.strip())
    # Filter out rows with empty/null author buckets
    author_data = author_data.dropna(subset=["AUTHOR_BUCKET"])
    author_data = author_data[author_data["AUTHOR_BUCKET"].str.strip() != ""]

    if author_data.empty:
        LOGGER.warning("No valid author bucket data found")
        # Create empty DataFrame with proper structure
        data = pd.DataFrame(columns=["Author_Bucket", "Count"])
    else:
        data = author_data.groupby(["AUTHOR_BUCKET"], as_index=False)[
            "COUNT"
        ].sum()
        # Define bucket order for proper sorting
        bucket_order = ["1", "2", "3", "4", "5+", "Unknown"]
        data["AUTHOR_BUCKET"] = pd.Categorical(
            data["AUTHOR_BUCKET"], categories=bucket_order, ordered=True
        )
        data = data.sort_values("AUTHOR_BUCKET")
        data.reset_index(drop=True, inplace=True)
        data.rename(
            columns={
                "AUTHOR_BUCKET": "Author_Bucket",
                "COUNT": "Count",
            },
            inplace=True,
        )

    file_path = shared.path_join(
        PATHS["data_phase"], "arxiv_totals_by_author_bucket.csv"
    )
    data_to_csv(args, data, file_path)


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])

    # Count data
    file1_count = shared.path_join(PATHS["data_1-fetch"], "arxiv_1_count.csv")
    count_data = pd.read_csv(file1_count, usecols=["TOOL_IDENTIFIER", "COUNT"])
    process_license_totals(args, count_data)

    # Category data
    file2_category = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_2_count_by_category_report.csv"
    )
    category_data = pd.read_csv(
        file2_category, usecols=["CATEGORY_CODE", "COUNT"]
    )
    process_category_totals(args, category_data)

    # Year data
    file3_year = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_3_count_by_year.csv"
    )
    year_data = pd.read_csv(file3_year, usecols=["YEAR", "COUNT"])
    process_year_totals(args, year_data)

    # Author bucket data
    file4_author = shared.path_join(
        PATHS["data_1-fetch"], "arxiv_4_count_by_author_bucket.csv"
    )
    author_data = pd.read_csv(file4_author, usecols=["AUTHOR_BUCKET", "COUNT"])
    process_author_bucket_totals(args, author_data)

    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new ArXiv data for {QUARTER}",
    )
    shared.git_push_changes(args, PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        if e.code != 0:
            LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        traceback_formatted = textwrap.indent(
            highlight(
                traceback.format_exc(),
                PythonTracebackLexer(),
                TerminalFormatter(),
            ),
            "    ",
        )
        LOGGER.critical(f"(1) Unhandled exception:\n{traceback_formatted}")
        sys.exit(1)
