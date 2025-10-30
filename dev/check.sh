#!/usr/bin/env bash
#
# Perform static analysis checks and reformat Python code using pre-commit
#
#### SETUP ####################################################################

set -o errexit
set -o errtrace
set -o nounset

# shellcheck disable=SC2154
trap '_es=${?};
    printf "${0}: line ${LINENO}: \"${BASH_COMMAND}\"";
    printf " exited with a status of ${_es}\n";
    exit ${_es}' ERR

DIR_REPO="$(cd -P -- "${0%/*}/.." && pwd -P)"
# https://en.wikipedia.org/wiki/ANSI_escape_code
E0="$(printf "\e[0m")"        # reset
E30="$(printf "\e[30m")"      # black foreground
E107="$(printf "\e[107m")"    # bright white background

#### FUNCTIONS ################################################################

print_header() {
    # Print 80 character wide black on white heading with time
    printf "${E30}${E107}# %-70s$(date '+%T') ${E0}\n" "${@}"
}

#### MAIN #####################################################################

cd "${DIR_REPO}"

print_header 'pre-commit'
echo 'See .pre-commit-config.yaml for pre-commit configuration.'
if [[ -n "${1:-}" ]]
then
    # Run on files specified on command line
    # shellcheck disable=SC2068
    pipenv run pre-commit run --files ${@:-}
else
    # Run on all files
    pipenv run pre-commit run --all-files
fi
echo
