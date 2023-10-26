# quantifying

Quantifying the Commons


## Overview

This project seeks to quantify the size and diversity of the commons--the
collection of works that are openly licensed or in the public domain.


## Code of conduct

[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md):
> The Creative Commons team is committed to fostering a welcoming community.
> This project and all other Creative Commons open source projects are governed
> by our [Code of Conduct][code_of_conduct]. Please report unacceptable
> behavior to [conduct@creativecommons.org](mailto:conduct@creativecommons.org)
> per our [reporting guidelines][reporting_guide].

[code_of_conduct]: https://opensource.creativecommons.org/community/code-of-conduct/
[reporting_guide]: https://opensource.creativecommons.org/community/code-of-conduct/enforcement/


## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).


## Development


### Prerequisites

This repository uses [pipenv][pipenvdocs] to manage the required Python
modules:
1. Install `pipenv`:
   - Linux: [Installing Pipenv][pipenvinstall]
   - macOS:
     1. Install [Homebrew][homebrew]
     2. Install pipenv:
        ```shell
        brew install pipenv
        ```
   - Windows: [Installing Pipenv][pipenvinstall]
2. Create the Python virtual environment and install prerequisites using
   `pipenv`:
    ```shell
    pipenv sync --dev
    ```

[pipenvdocs]: https://pipenv.pypa.io/en/latest/
[pipenvinstall]: https://pipenv.pypa.io/en/latest/installation/
[homebrew]: https://brew.sh/


### Running scripts that require client cedentials

To successfully run scripts that require client credentials, you will need to
follow these steps:
1. Copy the contents of the `env.example` file in the script's directory to
   `.env`:
    ```shell
    cp env.example .env
    ```
2. Uncomment the variables in the `.env` file and assign values as needed. See
   [`sources.md`](sources.md) on how to get credentials:
    ```
    GOOGLE_API_KEYS=your_api_key
    PSE_KEY=your_pse_key
    ```
3. Save the changes to the `.env` file.
4. You should now be able to run scripts that require client credentials
   without any issues.


### Static analysis

The [`dev/tools.sh`][tools-sh] helper script runs the static analysis tools
(`black`, `flake8`, and `isort`):
```shell
./dev/tools.sh
```

It can also accept command-line arguments to specify specific files or
directories to check:
```shell
./dev/tools.sh PATH/TO/MY/FILE.PY
```

[tools-sh]: /dev/tools.sh


### Resources

- **[Python Guidelines — Creative Commons Open Source][ccospyguide]**
- [Black][black]: _the uncompromising Python code formatter_
- [flake8][flake8]: _a python tool that glues together pep8, pyflakes, mccabe,
  and third-party plugins to check the style and quality of some python code._
- [isort][isort]: _A Python utility / library to sort imports_
  - (It doesn't import any libraries, it only sorts and formats them.)
- [ppypa/pipenv][pipenv]: _Python Development Workflow for Humans._

[ccospyguide]: https://opensource.creativecommons.org/contributing-code/python-guidelines/
[black]: https://github.com/psf/black
[flake8]: https://gitlab.com/pycqa/flake8
[isort]: https://pycqa.github.io/isort/
[pipenv]: https://github.com/pypa/pipenv


### GitHub Actions

The [`.github/workflows/python_static_analysis.yml`][workflow-static-analysis]
GitHub Actions workflow performs static analysis (`black`, `flake8`, and
`isort`) on committed changes. The workflow is triggered automatically when you
push changes to the main branch or open a pull request.

[workflow-static-analysis]: .github/workflows/python_static_analysis.yml


## Data sources

Kindly visit the [`sources.md`](sources.md) file for it.


## History

For information on past efforts, see [`history.md`](history.md).


## Copying & license


### Code

[`LICENSE`](LICENSE): the code within this repository is licensed under the
Expat/[MIT][mit] license.

[mit]: http://www.opensource.org/licenses/MIT "The MIT License | Open Source Initiative"


### Data

[![CC0 1.0 Universal (CC0 1.0) Public Domain Dedication
button][cc-zero-png]][cc-zero]

The data within this repository is dedicated to the public domain under the
[CC0 1.0 Universal (CC0 1.0) Public Domain Dedication][cc-zero].

[cc-zero-png]: https://licensebuttons.net/l/zero/1.0/88x31.png "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication button"
[cc-zero]: https://creativecommons.org/publicdomain/zero/1.0/


### Documentation

[![CC BY 4.0 license button][cc-by-png]][cc-by]

The documentation within the project is licensed under a [Creative Commons
Attribution 4.0 International License][cc-by].

[cc-by-png]: https://licensebuttons.net/l/by/4.0/88x31.png#floatleft "CC BY 4.0 license button"
[cc-by]: https://creativecommons.org/licenses/by/4.0/ "Creative Commons Attribution 4.0 International License"