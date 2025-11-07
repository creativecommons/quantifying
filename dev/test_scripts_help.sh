#!/usr/bin/env bash
#
# Ensure each script can display help message to ensure basic execution.
#
# This script must be run from within the pipenv shell or a properly configured
# environment.
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
EXIT_STATUS=0

#### FUNCTIONS ################################################################

test_help() {
    local _es _script
    for _script in $(find scripts/?-* -type f -name '*.py' | sort)
    do
        _es=0
        ./"${_script}" --help &>/dev/null || _es=${?}
        if (( _es == 0 ))
        then
            echo "✅ ${_script}"
        else
            echo "❌ ${_script}"
            EXIT_STATUS=${_es}
        fi
    done
}

#### MAIN #####################################################################

cd "${DIR_REPO}"
test_help
exit ${EXIT_STATUS}
