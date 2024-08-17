# Standard library
# import argparse
# Standard library
import logging
import os
from datetime import datetime, timezone

# Third-party
from git import InvalidGitRepositoryError, NoSuchPathError, Repo
from pandas import PeriodIndex


class QuantifyingException(Exception):
    def __init__(self, message, exit_code=None):
        self.exit_code = exit_code if exit_code else 1
        self.message = message
        super().__init__(self.message)


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
    paths["repo"] = os.path.dirname(
        os.path.abspath(os.path.realpath(os.path.join(__file__, "..")))
    )
    paths["dotenv"] = os.path.join(paths["repo"], ".env")
    paths["data"] = os.path.dirname(
        os.path.abspath(os.path.realpath(current_file))
    )
    phase = os.path.basename(
        os.path.dirname(os.path.abspath(os.path.realpath(current_file)))
    )
    paths["data"] = os.path.join(paths["repo"], "data")
    data_quarter = os.path.join(paths["data"], f"{quarter}")
    paths["state"] = os.path.join(data_quarter, "state.yaml")
    paths["data_phase"] = os.path.join(data_quarter, phase)

    paths["data_quarter"] = data_quarter

    return logger, paths


def log_paths(logger, paths):
    paths_list = []
    for label, path in paths.items():
        label = f"{label}:"
        paths_list.append(f"\n{' ' * 12}{label:<11} {path}")
    paths_list = "".join(paths_list)
    logger.info(f"PATHS:{paths_list}")


def fetch_and_merge(repo_path, branch="gsoc2024-dev-1"):
    try:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        origin.fetch()
        repo.git.merge(f"origin/{branch}", allow_unrelated_histories=True)
        logging.info(f"Fetched and merged latest changes from {branch}")
    except InvalidGitRepositoryError:
        raise QuantifyingException(f"Invalid Git repository at {repo_path}", 2)
    except NoSuchPathError:
        raise QuantifyingException(f"No such path: {repo_path}", 3)
    except Exception as e:
        raise QuantifyingException(f"Error during fetch and merge: {e}", 1)


def add_and_commit(repo_path, message):
    try:
        repo = Repo(repo_path)
        if not repo.is_dirty(untracked_files=True):
            logging.info("No changes to commit")
            return
        repo.git.add(update=True)
        repo.index.commit(message)
        logging.info("Changes committed")
    except InvalidGitRepositoryError:
        raise QuantifyingException(f"Invalid Git repository at {repo_path}", 2)
    except NoSuchPathError:
        raise QuantifyingException(f"No such path: {repo_path}", 3)
    except Exception as e:
        raise QuantifyingException(f"Error during add and commit: {e}", 1)


def push_changes(repo_path):
    try:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        origin.push()
        logging.info("Changes pushed")
    except InvalidGitRepositoryError:
        raise QuantifyingException(f"Invalid Git repository at {repo_path}", 2)
    except NoSuchPathError:
        raise QuantifyingException(f"No such path: {repo_path}", 3)
    except Exception as e:
        raise QuantifyingException(f"Error during push changes: {e}", 1)


def update_readme(
    paths, image_path, data_source, description, section_title, args
):
    """
    Update the README.md file with the generated images and descriptions.
    """
    readme_path = os.path.join(paths["data"], args.quarter, "README.md")
    section_marker_start = "<!-- GCS Start -->"
    section_marker_end = "<!-- GCS End -->"
    data_source_title = f"## Data Source: {data_source}"

    # Define section markers for each report type
    specific_section_start = f"<!-- {section_title} Start -->"
    specific_section_end = f"<!-- {section_title} End -->"
    data_source_title = f"## Data Source: {data_source}"

    # Convert image path to a relative path
    rel_image_path = os.path.relpath(image_path, os.path.dirname(readme_path))

    if os.path.exists(readme_path):
        with open(readme_path, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    # Main GCS Section
    section_start = None
    section_end = None
    for i, line in enumerate(lines):
        if section_marker_start in line:
            section_start = i
        if section_marker_end in line:
            section_end = i

    # Check if the main section is present
    if section_start is None or section_end is None:
        # If the main section is not present, add it
        lines.extend(
            [
                f"# {args.quarter} Quantifying the Commons\n",
                f"{section_marker_start}\n",
                f"{data_source_title}\n\n",
                f"{section_marker_end}\n",
            ]
        )
        section_start = len(lines) - 2
        section_end = len(lines) - 1

    # Locate the specific section markers within the main section
    specific_start = None
    specific_end = None
    for i in range(section_start, section_end):
        if specific_section_start in lines[i]:
            specific_start = i
        if specific_section_end in lines[i]:
            specific_end = i

    # Prepare the new content for this specific section
    new_content = [
        f"{specific_section_start}\n",
        f"### {section_title}\n",
        f"![{description}]({rel_image_path})\n",
        f"{description}\n",
        f"{specific_section_end}\n",
    ]

    # If the specific section is found, replace the content
    if specific_start is not None and specific_end is not None:
        # Replace the content between the specific markers
        lines = (
            lines[:specific_start]
            + new_content
            + lines[specific_end + 1 :]  # noqa: E203
        )
    else:
        # If specific section does not exist, add it before main end marker
        lines = lines[:section_end] + new_content + lines[section_end:]

    # Write back to the README.md file
    with open(readme_path, "w") as f:
        f.writelines(lines)

    logging.info(
        f"Updated {readme_path} with new image and"
        f"description for {section_title}."
    )


# def main():
#     parser = argparse.ArgumentParser(description="Git operations script")
#     parser.add_argument(
#         "--operation",
#         type=str,
#         required=True,
#         help="Operation to perform: fetch_and_merge, add_and_commit, push",
#     )
#     parser.add_argument("--message", type=str, help="Commit message")
#     parser.add_argument(
#         "--branch",
#         type=str,
#         default="refine-automation",
#         help="Branch to fetch and merge from",
#     )
#     args = parser.parse_args()

#     repo_path = os.getcwd() # Assuming the script runs in repo root

#     if args.operation == "fetch_and_merge":
#         fetch_and_merge(repo_path, args.branch)
#     elif args.operation == "add_and_commit":
#         if not args.message:
#             raise ValueError(
#                 "Commit message is required for add_and_commit operation"
#             )
#         add_and_commit(repo_path, args.message)
#     elif args.operation == "push":
#         push_changes(repo_path)
#     else:
#         raise ValueError("Unsupported operation")


# if __name__ == "__main__":
#     main()
