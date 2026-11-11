import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import unquote
from urllib.request import Request

import release


COMMIT = "a" * 40
OLD_COMMIT = "b" * 40
ROOT_TAG = "v1.0.3"
PROXY_TAG = "proxy/v1.0.1"
BINARY = b"original compiled proxy"
CHECKSUM = f"{hashlib.sha256(BINARY).hexdigest()}  proxy-linux-amd64\n".encode()


class FakeGitHub(release.GitHub):
    def __init__(self):
        self.releases = {}
        self.content = {}
        self.latest = {"tag_name": "v1.0.2"}
        self.writes = []
        self.fail_upload = None

    def download(self, asset):
        return self.content[asset["id"]]

    def add_release(self, tag, draft=False):
        item = {
            "id": len(self.releases) + 1, "tag_name": tag, "draft": draft,
            "prerelease": False, "assets": [], "upload_url": "",
            "html_url": f"https://github.com/{release.REPOSITORY}/releases/tag/{tag}",
        }
        item["upload_url"] = f"https://uploads.github.com/{item['id']}{{?name,label}}"
        self.releases[tag] = item
        return item

    def add_asset(self, item, name, content):
        asset = {"id": len(self.content) + 1, "name": name}
        item["assets"].append(asset)
        self.content[asset["id"]] = content
        return asset

    def request(self, method, path, data=None, **kwargs):
        if method == "GET" and path == "releases/latest":
            return self.latest
        if method == "GET" and path.startswith("releases/tags/"):
            item = self.releases.get(unquote(path.removeprefix("releases/tags/")))
            return item if item and not item["draft"] else None
        if method == "GET" and path.startswith("releases?per_page=100&page="):
            page = int(path.rsplit("=", 1)[1])
            return list(self.releases.values())[(page - 1) * 100:page * 100]
        if method == "GET" and path.startswith("releases/"):
            return next(r for r in self.releases.values() if path == f"releases/{r['id']}")
        if method == "POST" and path == "releases/generate-notes":
            return {"body": "Changes since v1.0.2"}
        self.writes.append((method, path, data))
        if method == "POST" and path == "releases":
            item = self.add_release(data["tag_name"])
            item.update(data)
            return item
        if method == "PATCH":
            item = next(r for r in self.releases.values() if path == f"releases/{r['id']}")
            item.update(data)
            if data.get("make_latest") == "true":
                self.latest = item
            return item
        if method == "POST" and path.startswith("https://uploads.github.com/"):
            identity, name = path.split("/")[-1].split("?name=")
            item = next(r for r in self.releases.values() if str(r["id"]) == identity)
            if self.fail_upload == (item["tag_name"], name):
                raise release.ReleaseError("Simulated upload failure")
            return self.add_asset(item, name, data)
        raise AssertionError((method, path, data, kwargs))


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        previous = Path.cwd()
        os.chdir(scratch.name)
        self.addCleanup(os.chdir, previous)
        Path("proxy").mkdir()
        Path("proxy/version.txt").write_text("1.0.1\n")
        self.directory = Path(scratch.name) / "assets"
        self.github = FakeGitHub()
        self.tags = {ROOT_TAG: COMMIT}
        self.start_patch("git", return_value=COMMIT)
        self.start_patch("remote_commit", side_effect=lambda tag: self.tags.get(tag))
        self.run = self.start_patch("subprocess.run", side_effect=self.run_command)
        self.version_command = self.start_patch(
            "subprocess.check_output", return_value=f"proxy 1.0.1 (commit {COMMIT})\n",
        )

    def start_patch(self, name, **kwargs):
        patcher = patch(f"release.{name}", **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def run_command(self, command, **kwargs):
        if command[:2] == ["go", "build"]:
            Path(command[command.index("-o") + 1]).write_bytes(BINARY)
        if command[:2] == ["git", "push"]:
            self.tags[command[-1].removeprefix("refs/tags/")] = COMMIT
        return subprocess.CompletedProcess(command, 0)

    def prepare(self):
        release.prepare(self.github, self.directory, ROOT_TAG, COMMIT)

    def publish(self):
        release.publish(self.github, self.directory, ROOT_TAG, COMMIT)

    def existing_proxy(self, commit=OLD_COMMIT):
        self.tags[PROXY_TAG] = commit
        self.version_command.return_value = f"proxy 1.0.1 (commit {commit})\n"
        item = self.github.add_release(PROXY_TAG)
        self.github.add_asset(item, release.ASSET, BINARY)
        self.github.add_asset(item, release.CHECKSUM, CHECKSUM)
        return item

    def test_new_proxy_is_built_and_both_releases_publish_verified_assets(self):
        self.prepare()
        build = next(call for call in self.run.call_args_list if call.args[0][:2] == ["go", "build"])
        self.assertIn(f"-s -w -X main.proxyVersion=1.0.1 -X main.proxyCommit={COMMIT}", build.args[0])
        self.assertEqual(build.kwargs["env"]["GOWORK"], "off")
        self.assertEqual(build.kwargs["env"]["CGO_ENABLED"], "0")
        self.assertNotIn(PROXY_TAG, self.tags)
        self.publish()
        self.assertEqual(self.tags[PROXY_TAG], COMMIT)
        for tag in (PROXY_TAG, ROOT_TAG):
            item = self.github.release(tag)
            self.assertFalse(item["draft"])
            contents = {a["name"]: self.github.download(a) for a in item["assets"]}
            self.assertEqual(contents, {release.ASSET: BINARY, release.CHECKSUM: CHECKSUM})
        self.assertEqual(self.github.release(PROXY_TAG)["make_latest"], "false")
        self.assertEqual(self.github.latest["tag_name"], ROOT_TAG)

    def test_existing_proxy_ignores_current_branch_and_is_never_rebuilt(self):
        self.existing_proxy()
        self.prepare()
        self.publish()
        self.run.assert_not_called()
        self.assertEqual(self.tags[PROXY_TAG], OLD_COMMIT)
        item = self.github.release(ROOT_TAG)
        self.assertIn(OLD_COMMIT, item["body"])
        self.assertEqual(self.github.download(item["assets"][0]), BINARY)
        writes = len(self.github.writes)
        self.prepare()
        self.publish()
        self.assertEqual(len(self.github.writes), writes)

    def test_tag_without_published_assets_fails_without_building(self):
        self.tags[PROXY_TAG] = OLD_COMMIT
        with self.assertRaisesRegex(release.ReleaseError, "Recover its original assets"):
            self.prepare()
        self.run.assert_not_called()
        self.assertEqual(self.github.writes, [])

    def test_missing_or_draft_canonical_assets_cannot_be_rebuilt(self):
        item = self.existing_proxy()
        item["assets"].pop()
        with self.assertRaisesRegex(release.ReleaseError, "missing"):
            self.prepare()
        item["draft"] = True
        with self.assertRaisesRegex(release.ReleaseError, "published stable"):
            self.prepare()
        self.run.assert_not_called()

    def test_corrupt_download_or_wrong_embedded_source_fails_before_publication(self):
        item = self.existing_proxy()
        self.github.content[item["assets"][0]["id"]] = b"corrupt"
        with self.assertRaisesRegex(release.ReleaseError, "checksum"):
            self.prepare()
        self.github.content[item["assets"][0]["id"]] = BINARY
        self.version_command.return_value = f"proxy 1.0.1 (commit {COMMIT})"
        with self.assertRaisesRegex(release.ReleaseError, "version/source mismatch"):
            self.prepare()
        self.assertEqual(self.github.writes, [])

    def test_partial_root_upload_stays_draft_and_retry_reuses_proxy(self):
        self.prepare()
        self.github.fail_upload = (ROOT_TAG, release.CHECKSUM)
        with self.assertRaisesRegex(release.ReleaseError, "upload failure"):
            self.publish()
        draft_id = self.github.releases[ROOT_TAG]["id"]
        self.assertTrue(self.github.release(ROOT_TAG)["draft"])
        self.assertEqual(self.github.latest["tag_name"], "v1.0.2")
        self.github.fail_upload = None
        self.run.reset_mock()
        self.prepare()
        self.publish()
        self.run.assert_not_called()
        self.assertFalse(self.github.release(ROOT_TAG)["draft"])
        self.assertEqual(len(self.github.release(ROOT_TAG)["assets"]), 2)
        self.assertEqual(self.github.release(ROOT_TAG)["id"], draft_id)
        root_creations = [
            data for method, path, data in self.github.writes
            if method == "POST" and path == "releases" and data["tag_name"] == ROOT_TAG
        ]
        self.assertEqual(len(root_creations), 1)

    def test_conflicting_root_or_concurrent_proxy_tag_prevents_writes(self):
        self.prepare()
        self.tags[ROOT_TAG] = OLD_COMMIT
        with self.assertRaisesRegex(release.ReleaseError, "official build"):
            self.publish()
        self.tags[ROOT_TAG] = COMMIT
        self.tags[PROXY_TAG] = OLD_COMMIT
        with self.assertRaisesRegex(release.ReleaseError, "appeared after preparation"):
            self.publish()
        self.assertEqual(self.github.writes, [])

    def test_publication_job_retry_reconciles_original_artifact_and_same_root_draft(self):
        self.prepare()
        plan = (self.directory / "release-plan.json").read_bytes()
        self.github.fail_upload = (ROOT_TAG, release.CHECKSUM)
        with self.assertRaisesRegex(release.ReleaseError, "upload failure"):
            self.publish()
        draft = self.github.release(ROOT_TAG)
        draft_id = draft["id"]
        self.github.fail_upload = None
        self.run.reset_mock()
        self.publish()
        self.run.assert_not_called()
        self.assertEqual((self.directory / "release-plan.json").read_bytes(), plan)
        self.assertEqual(self.github.release(ROOT_TAG)["id"], draft_id)
        self.assertFalse(draft["draft"])
        self.assertEqual(len(draft["assets"]), 2)
        self.assertEqual(self.github.latest["tag_name"], ROOT_TAG)
        self.assertEqual(
            len([w for w in self.github.writes if w[:2] == ("POST", "releases")]), 2,
        )

    def test_publication_job_retry_rejects_different_canonical_bytes(self):
        self.prepare()
        self.github.fail_upload = (ROOT_TAG, release.CHECKSUM)
        with self.assertRaisesRegex(release.ReleaseError, "upload failure"):
            self.publish()
        canonical = self.github.release(PROXY_TAG)
        self.github.content[canonical["assets"][0]["id"]] = b"different canonical binary"
        writes = len(self.github.writes)
        self.run.reset_mock()
        with self.assertRaisesRegex(release.ReleaseError, "refusing to overwrite"):
            self.publish()
        self.run.assert_not_called()
        self.assertEqual(len(self.github.writes), writes)
        self.assertTrue(self.github.release(ROOT_TAG)["draft"])

    def test_existing_asset_is_never_overwritten(self):
        self.existing_proxy()
        self.prepare()
        item = self.github.add_release(ROOT_TAG, draft=True)
        self.github.add_asset(item, release.ASSET, b"unexpected existing binary")
        with self.assertRaisesRegex(release.ReleaseError, "refusing to overwrite"):
            self.publish()
        self.assertTrue(item["draft"])
        self.assertEqual(self.github.writes, [])

    def test_existing_tag_with_partial_canonical_release_requires_recovery(self):
        self.prepare()
        self.github.fail_upload = (PROXY_TAG, release.CHECKSUM)
        with self.assertRaisesRegex(release.ReleaseError, "upload failure"):
            self.publish()
        self.assertIn(PROXY_TAG, self.tags)
        self.assertTrue(self.github.release(PROXY_TAG)["draft"])
        self.assertIsNone(self.github.release(ROOT_TAG))
        self.run.reset_mock()
        writes = len(self.github.writes)
        with self.assertRaisesRegex(release.ReleaseError, "Recover its original assets"):
            self.publish()
        self.assertEqual(len(self.github.writes), writes)
        with self.assertRaisesRegex(release.ReleaseError, "Recover its original assets"):
            self.prepare()
        self.run.assert_not_called()

    def test_prerelease_does_not_replace_latest(self):
        self.existing_proxy()
        tag = "v1.0.3-preview"
        self.tags[tag] = COMMIT
        release.prepare(self.github, self.directory, tag, COMMIT)
        release.publish(self.github, self.directory, tag, COMMIT)
        self.assertTrue(self.github.release(tag)["prerelease"])
        self.assertFalse(self.github.release(tag)["draft"])
        self.assertEqual(self.github.latest["tag_name"], "v1.0.2")


class TransportTests(unittest.TestCase):
    def test_release_lookup_finds_draft_after_first_page_and_reads_by_id(self):
        github = object.__new__(release.GitHub)
        draft = {"id": 101, "tag_name": ROOT_TAG, "draft": True}
        first_page = [{"id": n, "tag_name": f"v0.0.{n}"} for n in range(100)]
        with patch.object(github, "request", side_effect=[first_page, [draft], draft]) as request:
            self.assertEqual(github.release(ROOT_TAG), draft)
        self.assertEqual(
            [call.args for call in request.call_args_list],
            [
                ("GET", "releases?per_page=100&page=1"),
                ("GET", "releases?per_page=100&page=2"),
                ("GET", "releases/101"),
            ],
        )

    def test_ambiguous_release_lookup_refuses_to_select_one(self):
        github = object.__new__(release.GitHub)
        with patch.object(github, "request", return_value=[
            {"id": 1, "tag_name": ROOT_TAG, "draft": False},
            {"id": 2, "tag_name": ROOT_TAG, "draft": True},
        ]):
            with self.assertRaisesRegex(release.ReleaseError, "Multiple releases"):
                github.release(ROOT_TAG)

    def test_asset_redirect_does_not_forward_credentials_to_storage(self):
        request = Request("https://api.github.com/repos/example/releases/assets/1")
        request.add_header("Authorization", "Basic secret")
        redirected = release.AssetRedirect().redirect_request(
            request, None, 302, "Found", {}, "https://release-assets.githubusercontent.com/asset",
        )
        self.assertFalse(redirected.has_header("Authorization"))

    def test_only_not_found_is_treated_as_missing(self):
        github = object.__new__(release.GitHub)
        github.authorization = "Basic secret"
        with patch("release.build_opener") as opener:
            github.opener = opener.return_value
            github.opener.open.side_effect = HTTPError("https://api.github.com", 403, "Forbidden", {}, None)
            with self.assertRaisesRegex(release.ReleaseError, "HTTP 403"):
                github.request("GET", "releases/latest", missing=True)
            github.opener.open.side_effect = HTTPError("https://api.github.com", 404, "Not Found", {}, None)
            self.assertIsNone(github.request("GET", "releases/latest", missing=True))


if __name__ == "__main__":
    unittest.main()
