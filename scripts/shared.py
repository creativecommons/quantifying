# Standard library
import logging
import os
from datetime import datetime, timezone

# Third-party
import yaml
from git import InvalidGitRepositoryError, NoSuchPathError, Repo
from pandas import PeriodIndex

# Constants
STATUS_FORCELIST = [
    408,  # Request Timeout
    422,  # Unprocessable Content (Validation failed, endpoint spammed, etc.)
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
]
USER_AGENT = (
    "QuantifyingTheCommons/1.0 "
    "(https://github.com/creativecommons/quantifying)"
)


class QuantifyingException(Exception):
    def __init__(self, message, exit_code=None):
        self.exit_code = exit_code if exit_code is not None else 1
        self.message = message
        super().__init__(self.message)


def git_fetch_and_merge(args, repo_path, branch=None):
    if not args.enable_git:
        return
    try:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        origin.fetch()

        # Determine the branch to use
        if branch is None:
            # Use the current branch if no branch is provided
            branch = repo.active_branch.name if repo.active_branch else "main"

        # Ensure that the branch exists in the remote
        if f"origin/{branch}" not in [ref.name for ref in repo.refs]:
            raise ValueError(
                f"Branch '{branch}' does not exist in remote 'origin'"
            )

        repo.git.merge(f"origin/{branch}", allow_unrelated_histories=True)
        logging.info(f"Fetched and merged latest changes from {branch}")
    except InvalidGitRepositoryError:
        raise QuantifyingException(f"Invalid Git repository at {repo_path}", 2)
    except NoSuchPathError:
        raise QuantifyingException(f"No such path: {repo_path}", 3)
    except Exception as e:
        raise QuantifyingException(f"Error during fetch and merge: {e}", 1)


def git_add_and_commit(args, repo_path, add_path, message):
    if not args.enable_git:
        return args
    try:
        repo = Repo(repo_path)
        if not repo.is_dirty(untracked_files=True, path=add_path):
            relative_add_path = os.path.relpath(add_path, repo_path)
            logging.info(f"No changes to commit in: {relative_add_path}")
            args.enable_git = False
            return args
        repo.index.add([add_path])
        repo.index.commit(message)
        logging.info(f"Changes committed: {message}")
    except InvalidGitRepositoryError:
        raise QuantifyingException(f"Invalid Git repository at {repo_path}", 2)
    except NoSuchPathError:
        raise QuantifyingException(f"No such path: {repo_path}", 3)
    except Exception as e:
        raise QuantifyingException(f"Error during add and commit: {e}", 1)
    return args


def git_push_changes(args, repo_path):
    if not args.enable_git:
        return
    try:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        origin.push().raise_if_error()
        logging.info("Changes pushed")
    except InvalidGitRepositoryError:
        raise QuantifyingException(f"Invalid Git repository at {repo_path}", 2)
    except NoSuchPathError:
        raise QuantifyingException(f"No such path: {repo_path}", 3)
    except Exception as e:
        raise QuantifyingException(f"Error during push changes: {e}", 1)


def path_join(*paths):
    return os.path.abspath(os.path.realpath(os.path.join(*paths)))


def paths_log(logger, paths):
    paths_list = []
    repo_path = paths["repo"]
    for label, path in paths.items():
        label = f"{label}:"
        if label == "repo:":
            paths_list.append(f"\n{' ' * 4}{label} {path}")
        else:
            path_new = path.replace(repo_path, ".")
            paths_list.append(f"\n{' ' * 8}{label:<15} {path_new}")
    paths_list = "".join(paths_list)
    logger.info(f"PATHS:{paths_list}")


def paths_update(logger, paths, old_quarter, new_quarter):
    logger.info(f"Updating paths: replacing {old_quarter} with {new_quarter}")
    for label in [
        "data_1-fetch",
        "data_2-process",
        "data_3-report",
        "data_phase",
        "data_quarter",
    ]:
        paths[label] = paths[label].replace(old_quarter, new_quarter)
    return paths


def setup(current_file):
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Datetime
    datetime_today = datetime.now(timezone.utc)
    quarter = PeriodIndex([datetime_today.date()], freq="Q")[0]

    # Paths
    paths = {}
    paths["repo"] = os.path.dirname(path_join(__file__, ".."))
    paths["dotenv"] = path_join(paths["repo"], ".env")
    paths["data"] = os.path.dirname(
        os.path.abspath(os.path.realpath(current_file))
    )
    current_phase = os.path.basename(
        os.path.dirname(os.path.abspath(os.path.realpath(current_file)))
    )
    paths["data"] = path_join(paths["repo"], "data")
    data_quarter = path_join(paths["data"], f"{quarter}")
    for phase in ["1-fetch", "2-process", "3-report"]:
        paths[f"data_{phase}"] = path_join(data_quarter, phase)
    paths["data_phase"] = path_join(data_quarter, current_phase)

    paths["data_quarter"] = data_quarter

    return logger, paths


def get_arxiv_categories():
    """Get comprehensive ArXiv category taxonomy mapping."""
    # ArXiv's comprehensive category mapping
    categories = {
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
        # Economics
        "econ.EM": "Econometrics",
        "econ.GN": "General Economics",
        "econ.TH": "Theoretical Economics",
        # Electrical Engineering
        "eess.AS": "Audio and Speech Processing",
        "eess.IV": "Image and Video Processing",
        "eess.SP": "Signal Processing",
        "eess.SY": "Systems and Control",
        # High Energy Physics
        "hep-ex": "High Energy Physics - Experiment",
        "hep-lat": "High Energy Physics - Lattice",
        "hep-ph": "High Energy Physics - Phenomenology",
        "hep-th": "High Energy Physics - Theory",
        # Other Physics
        "astro-ph": "Astrophysics",
        "astro-ph.CO": "Cosmology and Nongalactic Astrophysics",
        "astro-ph.EP": "Earth and Planetary Astrophysics",
        "astro-ph.GA": "Astrophysics of Galaxies",
        "astro-ph.HE": "High Energy Astrophysical Phenomena",
        "astro-ph.IM": "Instrumentation and Methods for Astrophysics",
        "astro-ph.SR": "Solar and Stellar Astrophysics",
        "cond-mat.dis-nn": "Disordered Systems and Neural Networks",
        "cond-mat.mes-hall": "Mesoscale and Nanoscale Physics",
        "cond-mat.mtrl-sci": "Materials Science",
        "cond-mat.other": "Other Condensed Matter",
        "cond-mat.quant-gas": "Quantum Gases",
        "cond-mat.soft": "Soft Condensed Matter",
        "cond-mat.stat-mech": "Statistical Mechanics",
        "cond-mat.str-el": "Strongly Correlated Electrons",
        "cond-mat.supr-con": "Superconductivity",
        "gr-qc": "General Relativity and Quantum Cosmology",
        "nlin.AO": "Adaptation and Self-Organizing Systems",
        "nlin.CD": "Chaotic Dynamics",
        "nlin.CG": "Cellular Automata and Lattice Gases",
        "nlin.PS": "Pattern Formation and Solitons",
        "nlin.SI": "Exactly Solvable and Integrable Systems",
        "nucl-ex": "Nuclear Experiment",
        "nucl-th": "Nuclear Theory",
        "quant-ph": "Quantum Physics",
    }

    return categories


def load_arxiv_categories(data_dir=None):
    """Load ArXiv category mappings with fallback to comprehensive mapping."""
    categories = {}

    # Try loading from YAML file first
    if data_dir:
        yaml_path = os.path.join(data_dir, "arxiv_category_map.yaml")
        if os.path.exists(yaml_path):
            try:
                with open(yaml_path, "r", encoding="utf-8") as f:
                    categories = yaml.safe_load(f) or {}
                logging.info(
                    f"Loaded {len(categories)} categories from {yaml_path}"
                )
            except (yaml.YAMLError, IOError, OSError) as e:
                logging.warning(f"Failed to load category YAML: {e}")

    # Fallback to comprehensive mapping if no local categories
    if not categories:
        categories = get_arxiv_categories()
        if categories and data_dir:
            # Save fetched categories for future use
            try:
                os.makedirs(data_dir, exist_ok=True)
                yaml_path = os.path.join(data_dir, "arxiv_category_map.yaml")
                with open(yaml_path, "w", encoding="utf-8") as f:
                    yaml.dump(
                        categories, f, default_flow_style=False, sort_keys=True
                    )
                logging.info(
                    f"Saved {len(categories)} categories to {yaml_path}"
                )
            except (yaml.YAMLError, IOError, OSError) as e:
                logging.warning(f"Failed to save categories: {e}")

    return categories


def normalize_arxiv_category(code, categories=None):
    """Convert category code to human-readable label."""
    if not code or code == "Unknown":
        return code

    if categories and code in categories:
        return categories[code]

    # Fallback: use uppercase first part of code
    if "." in code:
        return code.split(".")[0].upper()

    return code


def update_readme(
    args,
    section_title,
    entry_title,
    image_path,
    image_caption,
    entry_text=None,
):
    """
    Update the README.md file with the generated images and descriptions.
    """
    if not args.enable_save:
        return
    if image_path and not image_caption:
        raise QuantifyingException(
            "The update_readme function requires an image caption if an image"
            " path is provided"
        )
    if not image_path and image_caption:
        raise QuantifyingException(
            "The update_readme function requires an image path if an image"
            " caption is provided"
        )

    logger = args.logger
    paths = args.paths

    readme_path = path_join(paths["data"], args.quarter, "README.md")

    # Define section markers for each data source
    section_start_line = f"<!-- {section_title} Start -->\n"
    section_end_line = f"<!-- {section_title} End -->\n"

    # Define entry markers for each plot (optional) and description
    entry_start_line = f"<!-- {entry_title} Start -->\n"
    entry_end_line = f"<!-- {entry_title} End -->\n"

    if os.path.exists(readme_path):
        with open(readme_path, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    title_line = f"# Quantifying the Commons {args.quarter}\n"
    if not lines or lines[0].strip() != title_line.strip():
        # Add the title if it is not present or is incorrect
        lines.insert(0, title_line)
        lines.insert(1, "\n")

    # We only need to know the position of the end to append new entries
    if section_start_line in lines:
        # Locate the data source section if it is already present
        section_end_index = lines.index(section_end_line)
    else:
        # Add the data source section if it is absent
        lines.extend(
            [
                f"{section_start_line}",
                "\n",
                "\n",
                f"## {section_title}\n",
                "\n",
                "\n",
                f"{section_end_line}",
                "\n",
            ]
        )
        section_end_index = lines.index(section_end_line)

    # Locate the entry if it is already present
    if entry_start_line in lines:
        entry_start_index = lines.index(entry_start_line)
        entry_end_index = lines.index(entry_end_line)
        # Include any trailing empty/whitespace-only lines
        while entry_end_index + 1 < len(lines):
            if not lines[entry_end_index + 1].strip():
                entry_end_index += 1
            else:
                break
    # Initalize variables of entry is not present
    else:
        entry_start_index = None
        entry_end_index = None

    # Create entry markdown content
    if image_path:
        relative_image_path = os.path.relpath(
            image_path, os.path.dirname(readme_path)
        )
        image = f"\n![{image_caption}]({relative_image_path})\n"
    else:
        image = ""
    if entry_text and image_caption:
        text = f"\n{image_caption}\n\n{entry_text}\n"
    elif entry_text:
        text = f"\n{entry_text}\n"
    elif image_caption:
        text = f"\n{image_caption}\n"
    else:
        text = ""
    entry_lines = [
        f"{entry_start_line}",
        "\n",
        f"### {entry_title}\n",
        image,
        text,
        "\n",
        f"{entry_end_line}",
        "\n",
        "\n",
    ]

    if entry_start_index is None:
        # Add entry to end of section
        lines = (
            lines[:section_end_index] + entry_lines + lines[section_end_index:]
        )
    else:
        # Replace entry
        lines = (
            lines[:entry_start_index]
            + entry_lines
            + lines[entry_end_index + 1 :]  # noqa: E203
        )

    # Write back to the README.md file
    with open(readme_path, "w") as f:
        f.writelines(lines)

    logger.info(f"README path: {readme_path.replace(paths['repo'], '.')}")
    logger.info(
        f"Updated README with new image and description for {entry_title}."
    )
