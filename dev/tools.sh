#!/bin/bash
#### SETUP ####################################################################
set -o errtrace
set -o nounset

printf "\e[1m\e[7m %-80s\e[0m\n" 'isort'
pipenv run isort ${@:-.}
echo

printf "\e[1m\e[7m %-80s\e[0m\n" 'black'
pipenv run black ${@:-.}
echo

printf "\e[1m\e[7m %-80s\e[0m\n" 'flake8'
pipenv run flake8 ${@:-.}
echo
