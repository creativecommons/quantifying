#!/usr/bin/env bash
#
# Reformat cut & pasted Google Collection values to YAML
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
SCRIPT_NAME="${0##*/}"
# https://en.wikipedia.org/wiki/ANSI_escape_code
E0="$(printf "\e[0m")"        # reset
E30="$(printf "\e[30m")"      # black foreground
E31="$(printf "\e[31m")"      # red foreground
E97="$(printf "\e[97m")"        # bright white foreground
E107="$(printf "\e[107m")"    # bright white background

#### FUNCTIONS ################################################################

check_gsed() {
    local _msg
    if ! gsed --version &>/dev/null
    then
        error_exit 'This script requires GNU sed to available as gsed'
    fi
}

error_exit() {
    # Echo error message and exit with error
    echo -e "${E31}ERROR:${E0} ${*}" 1>&2
    exit 1
}

print_header() {
    # Print 80 character wide black on white heading with time
    printf "${E30}${E107}# %-70s$(date '+%T') ${E0}\n" "${@}"
}

#### MAIN #####################################################################

cd "${DIR_REPO}"

check_gsed

print_header 'Convert cut & paste to YAML'
COUNTRY_FILE=data/gcs_country_collection.yaml
url_part_1=https://developers.google.com/custom-search/docs/
url_part_2=xml_results_appendices#country-collection-values
COUNTRY_URL="${url_part_1}${url_part_2}"
LANGUAGE_FILE=data/gcs_language_collection.yaml
url_part_1=https://developers.google.com/custom-search/docs/
url_part_2=xml_results_appendices#language-collection-values
LANGUAGE_URL="${url_part_1}${url_part_2}"
echo 'This script assumes that the files contain data copied from the XML API'
echo 'reference appendices | Programmable Search Engine | Google for'
echo 'Developers page:'
echo
echo "${E97}${COUNTRY_FILE}${E0}"
echo "${COUNTRY_URL}"
echo
echo "${E97}${LANGUAGE_FILE}${E0}"
echo "${LANGUAGE_URL}"
echo

print_header 'Update and reformat: Google Country Collection values'
echo "${COUNTRY_FILE}"
echo '  Remove any existing line comments and create backup'
gsed --in-place=.bak \
    -e'/^#/d' \
    "${COUNTRY_FILE}"
echo '  Add line comments at top of file'
gsed --in-place \
    -e"1s|^|# Reformatted with ./dev/${SCRIPT_NAME}\\n|" \
    -e'1s|^|#\n|' \
    -e"1s|^|# ${url_part_1}${url_part_2}\\n|" \
    -e'1s|^|# Based on:\n|' \
    "${COUNTRY_FILE}"
echo '  Reformat to YAML'
gsed --in-place --regexp-extended \
    -e's|^([A-Z])|- country: \1|' \
    -e's| \t|\n  cr: |' \
    "${COUNTRY_FILE}"
echo '  Done.'
echo

print_header 'Update and reformat: Google Language Collection values'
echo "${LANGUAGE_FILE}"
echo '  Remove any existing line comments and create backup'
gsed --in-place=.bak \
    -e'/^#/d' \
    "${LANGUAGE_FILE}"
echo '  Add line comments at top of file'
gsed --in-place \
    -e"1s|^|# Reformatted with ./dev/${SCRIPT_NAME}\\n|" \
    -e'1s|^|#\n|' \
    -e"1s|^|# ${url_part_1}${url_part_2}\\n|" \
    -e'1s|^|# Based on:\n|' \
    "${LANGUAGE_FILE}"
echo '  Reformat to YAML'
gsed --in-place --regexp-extended \
    -e's|^([A-Z])|- language: \1|' \
    -e's| \t|\n  lr: |' \
    "${LANGUAGE_FILE}"
echo '  Done.'
echo
