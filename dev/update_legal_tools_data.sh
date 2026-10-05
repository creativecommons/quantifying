#!/usr/bin/env bash
#
# Update CC Legal Tools metadata CSV from the CC Legal Tools Data repository on
# GitHub
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
E31="$(printf "\e[31m")"      # red foreground
E107="$(printf "\e[107m")"    # bright white background

#### FUNCTIONS ################################################################

print_header() {
    # Print 80 character wide black on white heading with time
    printf "${E30}${E107}# %-70s$(date '+%T') ${E0}\n" "${@}"
}

#### MAIN #####################################################################

cd "${DIR_REPO}"

print_header 'Download CC Legal Tools metadata CSV'
if curl \
    --fail \
    --output-dir data \
    --remote-name \
    --remove-on-error \
    --retry 3 \
    https://raw.githubusercontent.com/creativecommons/cc-legal-tools-data/refs/heads/main/config/cc-legal-tools.csv
then
    echo 'Done.'
    echo
else
    EXIT_STATUS=${?}
    echo "${E31}ERROR:${E0} Download failed (exist status: ${EXIT_STATUS})."
    echo
    exit ${EXIT_STATUS}
fi
