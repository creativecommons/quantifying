# Standard library
import argparse
import logging
import os
from datetime import datetime, timezone

# Third-party
from git import InvalidGitRepositoryError, NoSuchPathError, Repo
from pandas import PeriodIndex


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


def fetch_and_merge(repo_path):
    try:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        origin.fetch()
        repo.git.merge("origin/main")
        logging.info("Fetched and merged latest changes from remote")
    except InvalidGitRepositoryError:
        logging.error(f"Invalid Git repository at {repo_path}")
    except NoSuchPathError:
        logging.error(f"No such path: {repo_path}")
    except Exception as e:
        logging.error(f"Error during fetch and merge: {e}")


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
        logging.error(f"Invalid Git repository at {repo_path}")
    except NoSuchPathError:
        logging.error(f"No such path: {repo_path}")


def push_changes(repo_path):
    try:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        origin.push()
        logging.info("Changes pushed")
    except InvalidGitRepositoryError:
        logging.error(f"Invalid Git repository at {repo_path}")
    except NoSuchPathError:
        logging.error(f"No such path: {repo_path}")


def main():
    parser = argparse.ArgumentParser(description="Git operations script")
    parser.add_argument(
        "--operation",
        type=str,
        required=True,
        help="Operation to perform: add_and_commit, push",
    )
    parser.add_argument("--message", type=str, help="Commit message")
    args = parser.parse_args()

    repo_path = os.getcwd()

    if args.operation == "fetch_and_merge":
        fetch_and_merge(repo_path)
    elif args.operation == "add_and_commit":
        if not args.message:
            raise ValueError(
                "Commit message is required for add_and_commit operation"
            )
        add_and_commit(repo_path, args.message)
    elif args.operation == "push":
        push_changes(repo_path)
    else:
        raise ValueError("Unsupported operation")


if __name__ == "__main__":
    main()
