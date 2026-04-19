# TODO — django-fsspec publiceren op PyPI

## Blokkerende items

- [ ] **LICENSE copyright fixen** — staat nu op naam van "Josh Schneier, David Larlet, et al." (django-storages). Vervang door:
  ```
  Copyright (c) 2025, Bastiaan Roos
  All rights reserved.
  ```
  Gebruik de BSD-3-Clause template van https://opensource.org/license/bsd-3-clause

- [ ] **PyPI naam checken** — is `django-fsspec` nog vrij?
  ```bash
  curl -s https://pypi.org/pypi/django-fsspec/json | head -1
  ```
  `{"message":"Not Found"}` = vrij. Anders: kies alternatief (`django-fsspec-storage`, `django-fsspec2`, etc.)

## Metadata aanpassen (pyproject.toml)

- [ ] **`s3fs` van verplichte naar optionele dependency**:
  ```toml
  dependencies = [
      "Django>=5.0",
      "fsspec>=2025.2.0",
  ]
  [project.optional-dependencies]
  s3 = ["s3fs>=2025.2.0"]
  ```
  Gebruikers installeren dan `pip install django-fsspec[s3]` als ze S3 willen.

- [ ] **`project.urls` uitbreiden**:
  ```toml
  [project.urls]
  Homepage = "https://github.com/GetThePointGit/django-fsspec"
  Source = "https://github.com/GetThePointGit/django-fsspec"
  Issues = "https://github.com/GetThePointGit/django-fsspec/issues"
  Changelog = "https://github.com/GetThePointGit/django-fsspec/blob/main/CHANGELOG.rst"
  ```

- [ ] **GitHub repo aanmaken** (als die nog niet bestaat) — public, zodat de links werken

- [ ] **Versie bumpen** in `django_fsspec/__init__.py` naar `0.0.1a2` (of `0.1.0` als je productie-bereid bent). Pas ook `Development Status` classifier aan als je hoger dan alpha gaat.

## Pre-publish checks

- [ ] **Tests draaien**:
  ```bash
  cd backend_dev_packages/django-fsspec2
  pytest tests/test_nested_fs.py tests/test_transparent_fs.py tests/test_utils.py -v
  ```

- [ ] **Lint draaien**:
  ```bash
  ruff check django_fsspec/
  ```

- [ ] **Build testen**:
  ```bash
  pip install build twine
  rm -rf dist/ build/ *.egg-info/
  python -m build
  python -m twine check dist/*
  ```
  Verwacht: `PASSED` voor zowel de `.tar.gz` als de `.whl`.

## Accounts aanmaken (eenmalig, handmatig)

- [ ] **PyPI account**: https://pypi.org/account/register/ — verifieer e-mail, zet 2FA aan
- [ ] **Test PyPI account** (apart!): https://test.pypi.org/account/register/ — idem
- [ ] **API token aanmaken op Test PyPI**: Account settings > API tokens > "Add API token" (scope: entire account). Kopieer token (`pypi-...`).
- [ ] **API token aanmaken op PyPI** (pas na succesvolle test publish)

## Publish flow

### Eerste keer: test op Test PyPI

```bash
python -m twine upload --repository testpypi dist/*
# Username: __token__
# Password: <je Test PyPI token>
```

- [ ] Controleer de page: https://test.pypi.org/project/django-fsspec/
  - README markdown rendert correct?
  - Classifiers kloppen?
  - Versie klopt?
  - Links werken?

- [ ] Test installatie:
  ```bash
  pip install --index-url https://test.pypi.org/simple/ \
              --extra-index-url https://pypi.org/simple/ \
              django-fsspec
  python -c "from django_fsspec import FsspecStorage; print('OK')"
  ```

### Echte release

```bash
python -m twine upload dist/*
# Username: __token__
# Password: <je PyPI token>
```

- [ ] Verifieer: https://pypi.org/project/django-fsspec/
- [ ] Test: `pip install django-fsspec` op een schone omgeving

## Na publicatie

- [ ] **Drainworks vendored kopie weghalen** — vervang `backend_dev_packages/django-fsspec2` door een echte dependency:
  ```toml
  django-fsspec = ">=0.0.1a2"
  ```

- [ ] **Trusted Publishing opzetten** (optioneel, sterk aanbevolen) — zodat je geen API tokens meer nodig hebt. GitHub Actions publiceert dan automatisch bij een release tag. Zie `.github/workflows/publish.yml` voorbeeld in de README of PyPI docs: https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/

- [ ] **CHANGELOG bijhouden** — bij elke release een nieuwe sectie bovenaan met versie + datum

- [ ] **Overweeg extra optional deps**:
  ```toml
  gcs = ["gcsfs>=2025.2.0"]
  azure = ["adlfs>=2025.2.0"]
  all = ["s3fs>=2025.2.0", "gcsfs>=2025.2.0", "adlfs>=2025.2.0"]
  ```
