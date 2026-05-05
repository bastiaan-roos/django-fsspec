# Cross-FS `cp_file` / `mv` Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `NestedFileSystem.cp_file` (cross-fs path) en `NestedFileSystem.mv` (cross-fs path) verifieerbaar: standaard een size-check op het tmp-bestand én op de destination, plus een opt-in checksum-vergelijking met "graceful skip" voor backends die geen vergelijkbare checksum leveren. Tegelijk de `mv`-volgorde-bug fixen waarbij de source-`rm` plaatsvindt vóór de destination-existence is bevestigd.

**Architecture:** Twee defensieve lagen op de bestaande tmp-file streaming flow. Laag 1 = "free" size-check (we hebben de tmp-bytes al lokaal) en is altijd aan. Laag 2 = optioneel checksum-vergelijk (`verify_checksum=True`) waarbij we *gracieus skippen* wanneer beide kanten geen vergelijkbare string-checksum leveren — anders breken we lokale-FS en multipart-uploads onnodig. Bij elke verificatie-mismatch verwijderen we de **destination** (verdacht), nooit de source (intact). De `mv`-cross-fs flow erft deze garantie omdat hij `cp_file` aanroept en pas dáárna `rm` doet. We voegen een tweede assertie toe (`fs2.exists(dest)` na `cp_file`) als belt-and-braces tegen toekomstige bugs.

**Tech Stack:** Python 3.11+, fsspec ≥ 2025.2.0, pytest. Geen nieuwe runtime-dependencies. Tests draaien tegen `local`/`dirfs` en `MemoryFileSystem` (geen S3 nodig — die suite blijft een aparte integration run).

**Branch hygiene:** Werk op een nieuwe feature-branch (`feat/cp-mv-verification`), niet op `main`. De huidige branch `fix/0.1.2-followups` is een 0.1.2-stabilisatiebranch — deze verandering is een nieuwe feature en hoort daar niet bovenop. Bij twijfel: forken vanaf `main`.

**Out of scope (separate follow-up plans):**
- Content-MD5 header in `FsspecStorage._stream_to_filesystem` (#3) — vereist fs-call signature changes en S3-specifieke `put_object`-route.
- `ChecksummedFile` helper utility (#5) — alleen zinvol nadat #3 erin zit.
- Default-on `verify_checksum` voor S3-backends (#6) — API-breaking, hoort bij 0.2.0 en vereist een aparte migratie-discussie.

---

## File Structure

**Modify:**
- `django_fsspec/nested_fs.py` — voeg module-constante toe; herschrijf `cp_file` (cross-fs branch) en `mv` (cross-fs branch); voeg een private helper `_compare_checksums_safe` toe.
- `tests/test_nested_fs.py` — nieuwe `TestCpFileCrossFsVerification` en `TestMvCrossFsVerification` klassen.
- `CHANGELOG.rst` — voeg entry toe onder "0.1.2 (unreleased)" (of een nieuwe "0.2.0" sectie afhankelijk van of dit nog mee kan met 0.1.2 — zie Task 7).

**Geen nieuwe modules.** Alle logica blijft binnen `nested_fs.py` zodat de blast radius minimaal is en de helper niet zonder use-case in `utils.py` rondzwerft (YAGNI).

---

## Task 1: Module-level multipart cutoff constante

**Files:**
- Modify: `django_fsspec/nested_fs.py:1-18` (imports/top-of-file)

- [ ] **Step 1: Voeg de constante toe direct ná de imports**

In `django_fsspec/nested_fs.py`, direct ná de `if TYPE_CHECKING:` block (regel 17-18) en vóór de `class NestedFileSystem` regel:

```python
# Files larger than this threshold are typically uploaded to S3 as multipart
# objects, which makes the ETag a `"{md5}-{partcount}"` digest instead of the
# plain MD5 of the bytes. That digest is not portable across backends, so a
# raw checksum-vs-checksum comparison would always mismatch above this cut-off.
# We conservatively skip checksum verification for files this size or larger
# and rely on size-only verification (which is always meaningful).
#
# The 5 MiB value is the AWS default `multipart_threshold`; AWS CLI uses 16 MiB
# and some tools 8 MiB. Keep this as a single constant so future tuning is
# one edit.
_NON_MULTIPART_LIMIT = 5 * 1024 * 1024
```

- [ ] **Step 2: Geen test nodig, dit is een constante**

Verificatie via `python -c`:

```bash
pixi run python -c "from django_fsspec.nested_fs import _NON_MULTIPART_LIMIT; print(_NON_MULTIPART_LIMIT)"
```

Verwachte output: `5242880`

- [ ] **Step 3: Commit**

```bash
git add django_fsspec/nested_fs.py
git commit -m "add _NON_MULTIPART_LIMIT constant for cp_file checksum gating"
```

---

## Task 2: Private helper `_compare_checksums_safe`

**Files:**
- Modify: `django_fsspec/nested_fs.py` — voeg de helper toe als module-level private functie direct boven de `class NestedFileSystem` regel
- Test: `tests/test_nested_fs.py` — nieuwe testklasse `TestCompareChecksumsSafe` aan het einde van het bestand

- [ ] **Step 1: Schrijf de failing tests**

In `tests/test_nested_fs.py`, voeg aan het einde toe (vóór de file-EOF, na de bestaande `TestNextedPathFileSystem` klasse):

```python
import pytest

from django_fsspec.nested_fs import _compare_checksums_safe
from django_fsspec.nested_fs import _NON_MULTIPART_LIMIT


class TestCompareChecksumsSafe(unittest.TestCase):
    """Verifies the graceful-skip semantics of the checksum compare helper.

    The helper must:
    - return ``True`` when both checksums are equal strings,
    - raise ``IOError`` when both are strings but unequal,
    - return ``True`` (skip) when either fs returns a non-string checksum
      (e.g. local FS returns an int of size+mtime that always mismatches),
    - return ``True`` (skip) when either ``checksum()`` raises
      ``NotImplementedError``,
    - return ``True`` (skip) when ``size`` is at or above
      ``_NON_MULTIPART_LIMIT`` (multipart ETag uncertainty).
    """

    def _fake_fs(self, checksum_value):
        """Return an object exposing a ``checksum(path)`` method."""

        class _FakeFs:
            def checksum(self, path):
                if isinstance(checksum_value, type) and issubclass(checksum_value, BaseException):
                    raise checksum_value("nope")
                return checksum_value

        return _FakeFs()

    def test_matching_string_checksums(self):
        result = _compare_checksums_safe(
            self._fake_fs("abc"), "src", self._fake_fs("abc"), "dst", size=1024
        )
        self.assertTrue(result)

    def test_mismatched_string_checksums_raises(self):
        with self.assertRaises(IOError) as ctx:
            _compare_checksums_safe(
                self._fake_fs("abc"), "src", self._fake_fs("xyz"), "dst", size=1024
            )
        msg = str(ctx.exception)
        self.assertIn("Checksum mismatch", msg)
        self.assertIn("src", msg)
        self.assertIn("dst", msg)

    def test_int_checksum_skips(self):
        # Local FS returns int(size+mtime). isinstance(str) gating must skip.
        result = _compare_checksums_safe(
            self._fake_fs(123), "src", self._fake_fs("abc"), "dst", size=1024
        )
        self.assertTrue(result)

    def test_notimplementederror_skips(self):
        result = _compare_checksums_safe(
            self._fake_fs(NotImplementedError), "src", self._fake_fs("abc"), "dst", size=1024
        )
        self.assertTrue(result)

    def test_size_at_multipart_limit_skips(self):
        # File exactly at threshold: skip (defensive — could be multipart).
        result = _compare_checksums_safe(
            self._fake_fs("abc"), "src", self._fake_fs("xyz"), "dst", size=_NON_MULTIPART_LIMIT
        )
        self.assertTrue(result)

    def test_size_above_multipart_limit_skips(self):
        result = _compare_checksums_safe(
            self._fake_fs("abc"), "src", self._fake_fs("xyz"), "dst", size=_NON_MULTIPART_LIMIT * 2
        )
        self.assertTrue(result)
```

- [ ] **Step 2: Run tests om te bevestigen dat ze falen**

```bash
pixi run pytest tests/test_nested_fs.py::TestCompareChecksumsSafe -v
```

Verwachte output: 6 tests falen met `ImportError` op `_compare_checksums_safe`.

- [ ] **Step 3: Implementeer de helper**

In `django_fsspec/nested_fs.py`, vlak boven `class NestedFileSystem` (na de `_NON_MULTIPART_LIMIT` constante):

```python
def _compare_checksums_safe(fs1, path1: str, fs2, path2: str, *, size: int) -> bool:
    """Compare checksums of two paths across two filesystems with graceful skip.

    The compare is best-effort: filesystems that cannot produce a portable
    string checksum (local FS returns ``int(size+mtime)``; some backends
    raise ``NotImplementedError``) are skipped rather than treated as a
    failure, because a hard-fail would make this layer unusable for local
    development. Files at or above ``_NON_MULTIPART_LIMIT`` are also
    skipped: S3 multipart uploads change the ETag format
    (``"{md5}-{partcount}"``) and the comparison would always mismatch.

    Parameters
    ----------
    fs1, fs2 : fsspec.AbstractFileSystem
        Source and destination filesystems.
    path1, path2 : str
        Paths within the respective filesystems.
    size : int
        Size of the file in bytes — used to gate the multipart cut-off.

    Returns
    -------
    bool
        ``True`` when checksums matched OR the comparison was skipped.

    Raises
    ------
    IOError
        When both checksums are strings (i.e. comparable) but unequal.
        The destination is **not** removed here — the caller decides
        cleanup, because only the caller knows whether ``path2`` was
        freshly created by the current operation.
    """
    if size >= _NON_MULTIPART_LIMIT:
        # Multipart-uploaded objects use a different ETag formula on S3;
        # a portable comparison is not feasible, so size-check is the
        # strongest guarantee we can offer above this threshold.
        return True
    try:
        cs1 = fs1.checksum(path1)
        cs2 = fs2.checksum(path2)
    except NotImplementedError:
        # Some backends do not implement checksum() at all — that is fine,
        # we already verified size which catches the bulk of corruption.
        return True
    # Local FS returns int(size+mtime); two local FS roots always disagree
    # on mtime so this would be a guaranteed false-positive. Restrict the
    # comparison to portable string checksums.
    if not isinstance(cs1, str) or not isinstance(cs2, str):
        return True
    if cs1 != cs2:
        raise IOError(
            f"Checksum mismatch between {path1!r} and {path2!r}: {cs1!r} != {cs2!r}"
        )
    return True
```

- [ ] **Step 4: Run tests om te bevestigen dat ze slagen**

```bash
pixi run pytest tests/test_nested_fs.py::TestCompareChecksumsSafe -v
```

Verwachte output: 6 tests passeren.

- [ ] **Step 5: Commit**

```bash
git add django_fsspec/nested_fs.py tests/test_nested_fs.py
git commit -m "add _compare_checksums_safe helper with graceful skip semantics"
```

---

## Task 3: Size-check (always-on) in `cp_file` cross-fs branch

**Files:**
- Modify: `django_fsspec/nested_fs.py:346-363` (de `cp_file` methode)
- Test: `tests/test_nested_fs.py` — nieuwe testklasse `TestCpFileCrossFsVerification`

- [ ] **Step 1: Schrijf de failing tests**

In `tests/test_nested_fs.py` (na de helper-tests uit Task 2), voeg toe:

```python
class TestCpFileCrossFsVerification(unittest.TestCase):
    """Cross-fs cp_file must verify that the destination size matches the
    source. On mismatch the destination is removed; the source is left
    intact. Same-fs cp_file delegates to the sub-fs and is out of scope here.
    """

    def setUp(self):
        # Two separate local roots so cp_file takes the cross-fs branch.
        self.fs = NestedFileSystem(nested_mapping)
        with self.fs.open("a/source.txt", "w") as f:
            f.write("0123456789" * 100)  # 1000 bytes

    def tearDown(self):
        shutil.rmtree(test_data_dir, ignore_errors=True)

    def test_size_match_copies_successfully(self):
        """Happy path: sizes match → copy succeeds, both files present."""
        self.fs.cp_file("a/source.txt", "b/dest.txt")
        self.assertTrue(self.fs.exists("a/source.txt"))
        self.assertTrue(self.fs.exists("b/dest.txt"))
        self.assertEqual(self.fs.size("a/source.txt"), self.fs.size("b/dest.txt"))

    def test_size_mismatch_raises_and_removes_destination(self):
        """Mismatch: destination is removed, source is preserved, IOError raised."""
        # Get the destination sub-fs and monkey-patch its size() to lie
        # about the written bytes — simulates a partial put.
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_size = fs2.size

        def _lying_size(path, *args, **kwargs):
            return original_size(path, *args, **kwargs) - 1

        fs2.size = _lying_size
        try:
            with self.assertRaises(IOError) as ctx:
                self.fs.cp_file("a/source.txt", "b/dest.txt")
            self.assertIn("size", str(ctx.exception).lower())
        finally:
            fs2.size = original_size

        # Source intact, destination removed (per "delete the suspect, keep
        # the original" cleanup philosophy).
        self.assertTrue(self.fs.exists("a/source.txt"))
        self.assertFalse(self.fs.exists("b/dest.txt"))

    def test_destination_remove_failure_is_swallowed(self):
        """When the cleanup rm itself fails, the original IOError still surfaces.

        Use case: destination fs lost connectivity or permission was revoked
        between put_file and rm. We must not mask the verification error
        with the cleanup error.
        """
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_size = fs2.size
        fs2.size = lambda path, *a, **k: original_size(path, *a, **k) - 1

        original_rm = fs2.rm
        def _broken_rm(*a, **k):
            raise PermissionError("cleanup denied")
        fs2.rm = _broken_rm
        try:
            with self.assertRaises(IOError):
                self.fs.cp_file("a/source.txt", "b/dest.txt")
        finally:
            fs2.size = original_size
            fs2.rm = original_rm

    def test_same_fs_cp_file_unchanged(self):
        """Same-fs path delegates to the sub-fs and bypasses verification.

        Regression guard: we must not introduce verification overhead on
        the same-fs branch, because that breaks single-fs unit tests that
        do not configure size() to be reliable.
        """
        self.fs.cp_file("a/source.txt", "a/dest.txt")
        self.assertTrue(self.fs.exists("a/dest.txt"))
```

- [ ] **Step 2: Run tests om te bevestigen dat de mismatch-test faalt**

```bash
pixi run pytest tests/test_nested_fs.py::TestCpFileCrossFsVerification -v
```

Verwachte output: `test_size_mismatch_raises_and_removes_destination` en `test_destination_remove_failure_is_swallowed` falen (geen IOError wordt geraised in de huidige code). De andere twee passeren.

- [ ] **Step 3: Implementeer size-verification in `cp_file`**

Vervang in `django_fsspec/nested_fs.py` de bestaande `cp_file` methode (regels 346-363) door:

```python
    def cp_file(self, path1, path2, *, verify_checksum=False, **kwargs):
        """Copy ``path1`` to ``path2`` across (possibly different) sub-fs's.

        Same-fs copies are delegated to the sub-fs unchanged.

        Cross-fs copies stream through a local tempfile and are verified:

        - **Size-check (always on):** the destination size must equal the
          source size after upload. On mismatch the destination is removed
          and ``IOError`` is raised. The source is never touched.
        - **Checksum-check (opt-in via ``verify_checksum=True``):** in
          addition to the size-check, compares
          ``fs1.checksum(path1)`` against ``fs2.checksum(path2)`` with
          graceful skip for backends that do not return a portable string
          checksum or for files at/above ``_NON_MULTIPART_LIMIT``.

        Parameters
        ----------
        path1, path2 : str
            Source and destination paths in nested notation.
        verify_checksum : bool, optional
            Default ``False``. When ``True``, perform an additional checksum
            comparison after the size-check.
        **kwargs
            Forwarded to the underlying ``put_file`` call.

        Raises
        ------
        FileNotFoundError
            When either side does not have a sub-fs and there is no
            ``default``.
        IOError
            On size or checksum mismatch after copy. The destination is
            removed; the source is preserved.
        """
        fs1, _root1, nested_path1 = self._get_filesystem(path1)
        fs2, _root2, nested_path2 = self._get_filesystem(path2)
        if fs1 is None or fs2 is None:
            raise FileNotFoundError(f"No backend filesystem for {path1} or {path2}")
        if fs1 is fs2:
            return fs1.cp_file(nested_path1, nested_path2, **kwargs)

        # Cross-filesystem copy: stream via a temporary local path.
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            fs1.get_file(nested_path1, tmp_path)
            tmp_size = os.path.getsize(tmp_path)
            result = fs2.put_file(tmp_path, nested_path2, **kwargs)

            self._verify_after_cross_fs_copy(
                fs1=fs1, path1=nested_path1,
                fs2=fs2, path2=nested_path2,
                expected_size=tmp_size,
                verify_checksum=verify_checksum,
            )
            return result
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    def _verify_after_cross_fs_copy(
        self, *, fs1, path1, fs2, path2, expected_size, verify_checksum
    ):
        """Verify a freshly-written cross-fs destination; clean up on failure.

        Always size-checks; optionally checksum-checks. On any mismatch the
        destination is removed (best-effort — a failed cleanup does not
        mask the original mismatch error) and the verification error is
        re-raised. The source is never touched here.

        Parameters
        ----------
        fs1, fs2 : fsspec.AbstractFileSystem
            Source and destination filesystems.
        path1, path2 : str
            Paths within the respective filesystems.
        expected_size : int
            Authoritative byte count taken from the local tempfile after
            ``get_file``.
        verify_checksum : bool
            When ``True`` and the file is below ``_NON_MULTIPART_LIMIT``,
            also compare portable string checksums with graceful skip.
        """
        try:
            actual_size = fs2.size(path2)
            if actual_size != expected_size:
                raise IOError(
                    f"Size mismatch after copy of {path1!r} → {path2!r}: "
                    f"expected {expected_size} bytes, got {actual_size}."
                )
            if verify_checksum:
                _compare_checksums_safe(
                    fs1, path1, fs2, path2, size=expected_size,
                )
        except IOError:
            # Cleanup the suspect destination; the source is intact.
            # Swallow cleanup errors so we do not mask the verification
            # error itself.
            try:
                fs2.rm(path2)
            except Exception:
                pass
            raise
```

- [ ] **Step 4: Run tests om te bevestigen dat ze passeren**

```bash
pixi run pytest tests/test_nested_fs.py::TestCpFileCrossFsVerification -v
```

Verwachte output: alle 4 tests passeren.

- [ ] **Step 5: Run de volledige `nested_fs` testsuite om regressies te vangen**

```bash
pixi run pytest tests/test_nested_fs.py -v
```

Verwachte output: alle bestaande tests + de nieuwe blijven groen. In het bijzonder `test_cp_file_cross_fs` en `test_mv_cross_fs` (de bestaande happy-path tests).

- [ ] **Step 6: Commit**

```bash
git add django_fsspec/nested_fs.py tests/test_nested_fs.py
git commit -m "size-check cross-fs cp_file; remove dest on mismatch, keep source"
```

---

## Task 4: Optionele checksum-verify in `cp_file` met graceful skip

**Files:**
- Test: `tests/test_nested_fs.py` — extend `TestCpFileCrossFsVerification` (geen nieuwe class)

De productie-code is in Task 3 al geschreven (de `verify_checksum=True` parameter). Hier voegen we alleen end-to-end tests toe die bewijzen dat de optie de helper goed bedraadt.

- [ ] **Step 1: Schrijf de failing tests**

Voeg aan de `TestCpFileCrossFsVerification` klasse toe:

```python
    def test_verify_checksum_true_with_string_match_passes(self):
        """End-to-end: when both fs return matching string checksums, it succeeds."""
        fs1, _, _ = self.fs._get_filesystem("a/source.txt")
        fs2, _, _ = self.fs._get_filesystem("b/dest.txt")
        # Force a portable string checksum on both sides.
        fs1.checksum = lambda path, **k: "deadbeef"
        fs2.checksum = lambda path, **k: "deadbeef"

        self.fs.cp_file("a/source.txt", "b/dest.txt", verify_checksum=True)
        self.assertTrue(self.fs.exists("b/dest.txt"))

    def test_verify_checksum_true_with_string_mismatch_raises(self):
        """End-to-end: differing string checksums raise and remove dest."""
        fs1, _, _ = self.fs._get_filesystem("a/source.txt")
        fs2, _, _ = self.fs._get_filesystem("b/dest.txt")
        fs1.checksum = lambda path, **k: "deadbeef"
        fs2.checksum = lambda path, **k: "cafef00d"

        with self.assertRaises(IOError) as ctx:
            self.fs.cp_file("a/source.txt", "b/dest.txt", verify_checksum=True)
        self.assertIn("Checksum mismatch", str(ctx.exception))
        # Destination removed, source intact.
        self.assertFalse(self.fs.exists("b/dest.txt"))
        self.assertTrue(self.fs.exists("a/source.txt"))

    def test_verify_checksum_true_with_int_checksums_skips(self):
        """Default local-FS behavior (int checksum) must not break verify_checksum.

        Reason: the local FS returns ``int(size+mtime)`` which is guaranteed
        to differ between two roots. A naive comparison would make
        ``verify_checksum=True`` unusable for local development; the
        graceful-skip path in ``_compare_checksums_safe`` filters out
        non-string checksums.
        """
        # No monkey-patch: real local FS returns int. Should still succeed.
        self.fs.cp_file("a/source.txt", "b/dest.txt", verify_checksum=True)
        self.assertTrue(self.fs.exists("b/dest.txt"))

    def test_verify_checksum_default_off_does_not_call_checksum(self):
        """Without verify_checksum=True the helper must not be invoked.

        Guards against a regression where someone refactors and accidentally
        wires the checksum compare in unconditionally — we already had a
        bug like that in the FsspecStorage layer, document the contract.
        """
        fs1, _, _ = self.fs._get_filesystem("a/source.txt")
        calls = []
        original = fs1.checksum
        fs1.checksum = lambda path, **k: (calls.append(path) or original(path, **k))
        try:
            self.fs.cp_file("a/source.txt", "b/dest.txt")  # default off
        finally:
            fs1.checksum = original
        self.assertEqual([], calls, "checksum() must not be called when verify_checksum=False")
```

- [ ] **Step 2: Run tests om te bevestigen dat ze allemaal passeren**

De productie-code is al af in Task 3, dus we verwachten dat deze tests direct slagen. Als ze niet slagen, is dat een signaal dat de Task-3-implementatie iets mist:

```bash
pixi run pytest tests/test_nested_fs.py::TestCpFileCrossFsVerification -v
```

Verwachte output: alle 8 tests passeren.

- [ ] **Step 3: Commit**

```bash
git add tests/test_nested_fs.py
git commit -m "test verify_checksum=True end-to-end on cp_file with graceful skip"
```

---

## Task 5: `mv` bevestigt destination existence vóór `rm`

**Files:**
- Modify: `django_fsspec/nested_fs.py:369-378` (de `mv` methode)
- Test: `tests/test_nested_fs.py` — nieuwe testklasse `TestMvCrossFsVerification`

- [ ] **Step 1: Schrijf de failing tests**

Voeg aan `tests/test_nested_fs.py` toe (na `TestCpFileCrossFsVerification`):

```python
class TestMvCrossFsVerification(unittest.TestCase):
    """Cross-fs mv must not delete the source until the destination is
    confirmed present. The previous implementation called ``rm`` on the
    source unconditionally after ``cp_file`` — a silently-failing copy
    would have caused data loss.
    """

    def setUp(self):
        self.fs = NestedFileSystem(nested_mapping)
        with self.fs.open("a/source.txt", "w") as f:
            f.write("inhoud om te verplaatsen")

    def tearDown(self):
        shutil.rmtree(test_data_dir, ignore_errors=True)

    def test_happy_path_moves(self):
        """Sanity: a normal cross-fs mv still works."""
        self.fs.mv("a/source.txt", "b/dest.txt")
        self.assertFalse(self.fs.exists("a/source.txt"))
        self.assertTrue(self.fs.exists("b/dest.txt"))

    def test_destination_missing_after_copy_preserves_source(self):
        """If cp_file silently produced no destination, mv must not rm source.

        Simulated by monkey-patching the destination sub-fs ``exists`` to
        return False even after a successful put. In real life this would
        be e.g. a backend that swallows write errors; we want belt-and-
        braces against that.
        """
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_exists = fs2.exists

        def _lying_exists(path, *args, **kwargs):
            # Lie about the freshly-written file only; let real lookups pass.
            if path.endswith("dest.txt"):
                return False
            return original_exists(path, *args, **kwargs)

        fs2.exists = _lying_exists
        try:
            with self.assertRaises(IOError) as ctx:
                self.fs.mv("a/source.txt", "b/dest.txt")
            self.assertIn("destination", str(ctx.exception).lower())
        finally:
            fs2.exists = original_exists

        # Source must still be there — the operation failed before rm.
        self.assertTrue(self.fs.exists("a/source.txt"))

    def test_cp_file_failure_preserves_source(self):
        """If cp_file itself raises (e.g. size-check fails), mv must not rm source.

        Regression for the original bug: previously rm ran unconditionally
        after cp_file, even if cp_file later started raising on mismatch.
        """
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_size = fs2.size
        fs2.size = lambda path, *a, **k: 0  # always lie → size mismatch

        try:
            with self.assertRaises(IOError):
                self.fs.mv("a/source.txt", "b/dest.txt")
        finally:
            fs2.size = original_size

        self.assertTrue(self.fs.exists("a/source.txt"))
        self.assertFalse(self.fs.exists("b/dest.txt"))
```

- [ ] **Step 2: Run tests om te bevestigen dat de mv-tests falen**

```bash
pixi run pytest tests/test_nested_fs.py::TestMvCrossFsVerification -v
```

Verwachte output: `test_destination_missing_after_copy_preserves_source` faalt (huidige `mv` controleert geen exists). De andere twee passeren al — de tweede dankzij de cp_file-fix uit Task 3, de derde mogelijk afhankelijk van order. Documenteer welke falen in de output.

- [ ] **Step 3: Update `mv` om destination te checken vóór `rm`**

Vervang in `django_fsspec/nested_fs.py` de bestaande `mv` methode (de cross-fs branch) door:

```python
    def mv(self, path1, path2, *, verify_checksum=False, **kwargs):
        """Move ``path1`` to ``path2`` across (possibly different) sub-fs's.

        Same-fs moves are delegated to the sub-fs unchanged.

        Cross-fs moves are implemented as ``cp_file`` followed by ``rm`` of
        the source. The source ``rm`` runs only if:

        1. ``cp_file`` did not raise (which already covers size-mismatch
           and any opt-in checksum-mismatch via Task 3),
        2. AND the destination is observably present afterwards
           (belt-and-braces against backends that silently swallow write
           failures and do not propagate them as exceptions).

        If either condition fails the source is left intact and an
        ``IOError`` is raised; the caller can retry.

        Parameters
        ----------
        path1, path2 : str
            Source and destination paths in nested notation.
        verify_checksum : bool, optional
            Forwarded to ``cp_file``. Default ``False``.
        **kwargs
            Forwarded to ``cp_file`` / ``put_file``.

        Raises
        ------
        FileNotFoundError
            When either side does not have a sub-fs and there is no
            ``default``.
        IOError
            On any verification failure during the copy or when the
            destination is unexpectedly absent after the copy.
        """
        fs1, _root1, nested_path1 = self._get_filesystem(path1)
        fs2, _root2, nested_path2 = self._get_filesystem(path2)
        if fs1 is None or fs2 is None:
            raise FileNotFoundError(f"No backend filesystem for {path1} or {path2}")
        if fs1 is fs2:
            return fs1.mv(nested_path1, nested_path2, **kwargs)

        # Cross-filesystem move: cp (with verification), confirm, then rm.
        self.cp_file(path1, path2, verify_checksum=verify_checksum, **kwargs)
        if not fs2.exists(nested_path2):
            raise IOError(
                f"mv aborted: destination {path2!r} not present after copy; "
                f"source {path1!r} preserved."
            )
        return fs1.rm(nested_path1)
```

- [ ] **Step 4: Run de mv-tests + de hele nested-suite**

```bash
pixi run pytest tests/test_nested_fs.py::TestMvCrossFsVerification -v
pixi run pytest tests/test_nested_fs.py -v
```

Verwachte output: `TestMvCrossFsVerification` 3/3 passeert. Volledige suite blijft groen.

- [ ] **Step 5: Commit**

```bash
git add django_fsspec/nested_fs.py tests/test_nested_fs.py
git commit -m "mv: confirm destination exists before removing source"
```

---

## Task 6: Volledige test- en lint-run

**Files:** geen wijzigingen, alleen verificatie.

- [ ] **Step 1: Run de volledige testsuite**

```bash
pixi run tests
```

Verwachte output: alle tests groen, coverage rapport. Geen regressies in `test_fsspec_storage.py` of `test_transparent_fs.py`.

- [ ] **Step 2: Run de linter**

```bash
pixi run lint
```

Verwachte output: `All checks passed!` (of vergelijkbaar). Bij ruff-violations: fix ze; let in het bijzonder op:

- ongebruikte imports (we hebben geen nieuwe nodig — alle wijzigingen zijn binnen bestaande modules)
- regelbreedte ≤ 120 (de docstrings zijn lang — wrap als nodig)
- isort: `_compare_checksums_safe` import in tests staat na de `import unittest` block

- [ ] **Step 3: Run de import-sortering en formattering**

```bash
pixi run style
```

Verwachte output: geen file changes (of de tool past automatisch aan). Bij wijzigingen, commit ze:

```bash
git add -p django_fsspec tests
git commit -m "auto-format after cp_file/mv verification work"
```

---

## Task 7: CHANGELOG-entry en versie-overweging

**Files:**
- Modify: `CHANGELOG.rst:1-23` (de "0.1.2 (unreleased)" sectie)

Beslis eerst: hoort dit in 0.1.2 of 0.1.3 / 0.2.0? Tegen-argumenten voor 0.1.2:

- 0.1.2 is qua scope (per de bestaande CHANGELOG entry) "behavior alignment and error-message polish on top of 0.1.1". Verificatie is een nieuwe feature, geen polish.
- De wijziging op `cp_file` heeft een nieuwe kwarg en raised nu `IOError` in scenario's waarin het eerder stilletjes succes returnde. Dat is een gedragsverandering die je niet in een patch wil.

Aanbeveling: maak hier een **0.1.3** of **0.2.0** sectie van. De `verify_checksum` kwarg is opt-in en defaults conservatief, dus 0.1.3 (minor) is verdedigbaar; size-check is altijd aan en maakt een eerder stille fout luid, dat is "feature-with-behavior-change".

- [ ] **Step 1: Voeg een nieuwe CHANGELOG-sectie toe**

In `CHANGELOG.rst`, vóór de "0.1.2 (unreleased)" sectie:

```rst
0.1.3 (unreleased)
------------------

Cross-filesystem ``cp_file`` / ``mv`` now verify the copy before reporting success.

- ``NestedFileSystem.cp_file`` (cross-fs branch) compares the destination
  size to the local tempfile size after upload. On mismatch the destination
  is removed and ``IOError`` is raised; the source is untouched. Same-fs
  copies are delegated to the sub-fs and are unchanged.
- New keyword-only argument ``verify_checksum=False`` on ``cp_file`` and
  ``mv``. When set to ``True`` it adds a portable-string-checksum compare
  on top of the size check, with graceful skip for backends that do not
  return string checksums (local FS returns ``int(size+mtime)``) or that
  raise ``NotImplementedError``. Files at or above ``_NON_MULTIPART_LIMIT``
  (5 MiB) skip the checksum compare because S3 multipart ETags are not
  portable.
- ``NestedFileSystem.mv`` (cross-fs branch) now confirms the destination
  exists after the copy before removing the source. If a backend silently
  produced no destination the source is preserved and ``IOError`` is
  raised — previous behavior was to ``rm`` the source unconditionally.
```

- [ ] **Step 2: Bump versie in `__init__.py`**

In `django_fsspec/__init__.py:1`:

```python
__version__ = "0.1.3a1"
```

(Alpha-suffix omdat dit niet meteen naar PyPI gaat — pas later naar `0.1.3` op release-moment.)

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.rst django_fsspec/__init__.py
git commit -m "0.1.3a1: cp_file/mv verification + changelog"
```

- [ ] **Step 4: Final sanity check**

```bash
pixi run tests && pixi run lint
git log --oneline -10
```

Verwachte output: groene tests, schone lint, en 6 commits met duidelijke boodschappen op deze branch.

---

## Out of Scope — Follow-up plans

Deze items kwamen langs in de prio-tabel maar horen niet in deze plan-iteratie:

- **#3 — Content-MD5 header in `_stream_to_filesystem`**. Dit raakt `FsspecStorage._stream_to_filesystem` (regel 338-351 in `fsspec_storage.py`) en vereist S3-specifieke route via `s3fs.put_object` of `_open(..., extra={'Metadata': ...})`. Geen `cp_file`-concern; aparte plan met eigen test-fixtures (echte S3 + memory-fs).
- **#5 — `ChecksummedFile` helper utility**. Een wrapper die op-de-fly MD5 berekent terwijl `chunks()`/`read()` doorloopt. Pas zinvol nadat #3 erin zit; anders heeft niemand iets aan een vooraf berekende checksum.
- **#6 — Default-on `verify_checksum` voor S3-backends**. API-breaking gedragsverandering; hoort bij 0.2.0 met een aparte deprecation-flow op `verify_checksum=False` als opt-out, en migratie-instructies in de README. Niet alleen voor cp_file, ook voor `_save`.

---

## Self-Review Checklist

**1. Spec coverage:**
- [x] #1 (size-check cross-fs cp_file): Task 3.
- [x] #7 (mv confirms destination before rm): Task 5.
- [x] #2 (verify_checksum in cp_file/mv): Task 2 (helper) + Task 3 (cp_file kwarg) + Task 4 (tests) + Task 5 (mv kwarg).
- [x] Tip 2 (graceful skip): `_compare_checksums_safe` (Task 2).
- [x] Tip 3 (delete destination, not source): `_verify_after_cross_fs_copy` (Task 3) + cp_file failure preserves source (Task 5 test).
- [x] Tip 4 (module-level cutoff constant): `_NON_MULTIPART_LIMIT` (Task 1).
- [x] Tip 5 (test skip-paths explicitly): Task 2 (helper) + Task 4 (end-to-end skip).
- [x] Tip 6 (verify=True parameter): kwarg op `cp_file`/`mv` (Task 3 + Task 5).
- [x] Tip 8 (document WHY of skip-conditions): docstring van `_compare_checksums_safe` legt iedere skip uit.

#3, #5, #6 zijn bewust uit scope (zie "Out of Scope" sectie boven).

**2. Placeholder scan:** Geen `TODO`, `TBD`, "implement later", "similar to Task N", "fill in details" of "appropriate error handling" gebruikt. Alle codeblokken zijn compleet.

**3. Type consistency:**
- `_compare_checksums_safe(fs1, path1, fs2, path2, *, size: int) -> bool` — dezelfde signatuur in Task 2 (definitie) en Task 3 (call site).
- `cp_file(self, path1, path2, *, verify_checksum=False, **kwargs)` — dezelfde signatuur in Task 3 en in de mv-call uit Task 5 (`self.cp_file(path1, path2, verify_checksum=verify_checksum, **kwargs)`).
- `_verify_after_cross_fs_copy` — keyword-only args matchen tussen definitie en aanroep in Task 3.
- `_NON_MULTIPART_LIMIT` — gedefinieerd in Task 1, gebruikt in Task 2.
