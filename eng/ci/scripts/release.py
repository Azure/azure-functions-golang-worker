#!/usr/bin/env python3
"""Build or reuse a versioned proxy, then publish verified release assets."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import quote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener


REPOSITORY = "Azure/azure-functions-golang-worker"
ORIGIN = f"https://github.com/{REPOSITORY}"
ASSET = "proxy-linux-amd64"
CHECKSUM = f"{ASSET}.sha256"
VERSION = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"


class ReleaseError(RuntimeError):
    pass


class AssetRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlparse(newurl)
        if target.scheme != "https" or target.hostname not in (
            "api.github.com", "release-assets.githubusercontent.com",
        ):
            raise ReleaseError("Unexpected GitHub asset redirect destination")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if target.hostname != urlparse(req.full_url).hostname:
            redirected.remove_header("Authorization")
        return redirected


class GitHub:
    def __init__(self):
        # Reuse the repository-scoped checkout service connection, never log it.
        header = subprocess.check_output(
            ["git", "config", "--get-urlmatch", "http.extraheader", ORIGIN],
            text=True,
        ).strip()
        name, separator, value = header.partition(":")
        if not separator or name.lower() != "authorization" or "\n" in value:
            raise ReleaseError("Checkout has no usable GitHub Authorization header")
        self.authorization = value.strip()
        self.opener = build_opener(AssetRedirect())

    def request(self, method, path, data=None, *, missing=False, binary=False):
        if path.startswith("https://"):
            if urlparse(path).hostname != "uploads.github.com":
                raise ReleaseError("Unexpected GitHub upload destination")
            url = path
        else:
            url = f"https://api.github.com/repos/{REPOSITORY}/{path}"
        headers = {
            "Authorization": self.authorization,
            "Accept": "application/octet-stream" if binary else "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "azure-functions-golang-worker-release",
        }
        if isinstance(data, bytes):
            headers["Content-Type"] = "application/octet-stream"
        elif data is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(data).encode()
        try:
            with self.opener.open(Request(url, data=data, headers=headers, method=method), timeout=120) as response:
                body = response.read()
        except HTTPError as error:
            if missing and error.code == 404:
                return None
            raise ReleaseError(
                f"GitHub {method} {urlparse(url).path} failed with HTTP {error.code}. "
                "Check service connection Contents write permission and reconcile external state before retrying."
            ) from error
        return body if binary else json.loads(body)

    def release(self, tag):
        # The tag endpoint omits drafts, which must be reconciled on retries.
        matches = []
        page = 1
        while True:
            releases = self.request("GET", f"releases?per_page=100&page={page}")
            matches.extend(item for item in releases if item["tag_name"] == tag)
            if len(matches) > 1:
                raise ReleaseError(f"Multiple releases match {tag}; reconcile them before retrying")
            if len(releases) < 100:
                break
            page += 1
        return self.request("GET", f"releases/{matches[0]['id']}") if matches else None

    def download(self, asset):
        return self.request("GET", f"releases/assets/{asset['id']}", binary=True)


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def remote_commit(tag):
    result = subprocess.run(
        ["git", "ls-remote", "--exit-code", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"],
        text=True, capture_output=True,
    )
    if result.returncode == 2:
        return None
    result.check_returncode()
    refs = dict(line.split()[::-1] for line in result.stdout.splitlines())
    return refs.get(f"refs/tags/{tag}^{{}}", refs.get(f"refs/tags/{tag}"))


def assets_by_name(release):
    assets = {asset["name"]: asset for asset in release["assets"]}
    if len(assets) != len(release["assets"]):
        raise ReleaseError("Release contains duplicate asset names")
    return assets


def verify_files(directory, version, commit):
    binary = directory / ASSET
    expected = f"{hashlib.sha256(binary.read_bytes()).hexdigest()}  {ASSET}\n"
    if (directory / CHECKSUM).read_bytes() != expected.encode():
        raise ReleaseError("Proxy checksum does not match the binary")
    binary.chmod(0o755)
    output = subprocess.check_output([str(binary.resolve()), "--version"], text=True).strip()
    if output != f"proxy {version} (commit {commit})":
        raise ReleaseError(f"Proxy version/source mismatch: {output!r}")
    print(output)


def download_proxy(github, release, directory):
    if release is None or release["draft"] or release["prerelease"]:
        raise ReleaseError("Proxy tag exists without a published stable proxy release. Recover its original assets; do not rebuild this version.")
    assets = assets_by_name(release)
    for name in (ASSET, CHECKSUM):
        if name not in assets:
            raise ReleaseError(f"Proxy release is missing {name}. Recover the original asset; do not rebuild this version.")
        (directory / name).write_bytes(github.download(assets[name]))


def prepare(github, directory, root_tag, commit):
    if git("rev-parse", "HEAD") != commit:
        raise ReleaseError("Checkout does not match the selected official build commit")
    version = Path("proxy/version.txt").read_text().strip()
    if not re.fullmatch(VERSION, version):
        raise ReleaseError("proxy/version.txt must contain a stable version such as 1.0.1")
    if not re.fullmatch(f"v{VERSION}(-[A-Za-z0-9.-]+)?", root_tag):
        raise ReleaseError("Invalid Go worker release tag")
    tag = f"proxy/v{version}"
    source = remote_commit(tag)
    reused = source is not None
    directory.mkdir(parents=True, exist_ok=True)
    if source:
        print(f"Reusing {tag} from {source}; current proxy source will not be built.")
        download_proxy(github, github.release(tag), directory)
    else:
        if github.release(tag) is not None:
            raise ReleaseError("Proxy release exists without its source tag; reconcile it before continuing")
        source = commit
        env = dict(os.environ, GOWORK="off", CGO_ENABLED="0", GOOS="linux", GOARCH="amd64", GOTOOLCHAIN="local")
        subprocess.run(["go", "test", "-mod=readonly", "./proxy"], env=env, check=True)
        subprocess.run([
            "go", "build", "-mod=readonly", "-trimpath",
            "-ldflags", f"-s -w -X main.proxyVersion={version} -X main.proxyCommit={source}",
            "-o", str(directory / ASSET), "./proxy",
        ], env=env, check=True)
        digest = hashlib.sha256((directory / ASSET).read_bytes()).hexdigest()
        (directory / CHECKSUM).write_bytes(f"{digest}  {ASSET}\n".encode())
    verify_files(directory, version, source)
    plan = {"root_tag": root_tag, "commit": commit, "version": version, "source": source, "reused": reused}
    (directory / "release-plan.json").write_text(json.dumps(plan, indent=2) + "\n")


def ensure_assets(github, release, directory):
    assets = assets_by_name(release)
    for name in (ASSET, CHECKSUM):
        content = (directory / name).read_bytes()
        if name not in assets:
            if not release["draft"]:
                raise ReleaseError(f"Published release is missing {name}; refusing to modify it")
            upload = release["upload_url"].split("{", 1)[0]
            asset = github.request("POST", f"{upload}?name={quote(name)}", content)
        else:
            asset = assets[name]
        if github.download(asset) != content:
            raise ReleaseError(f"Release asset {name} differs from the verified proxy; refusing to overwrite it")


def publish(github, directory, root_tag, commit):
    plan = json.loads((directory / "release-plan.json").read_text())
    if plan["root_tag"] != root_tag or plan["commit"] != commit or git("rev-parse", "HEAD") != commit:
        raise ReleaseError("Prepared assets do not belong to this release commit")
    version, source = plan["version"], plan["source"]
    verify_files(directory, version, source)
    if remote_commit(root_tag) != commit:
        raise ReleaseError("Go worker release tag does not match the official build")
    proxy_tag = f"proxy/v{version}"
    existing = remote_commit(proxy_tag)
    if plan["reused"] or existing == source:
        if existing != source:
            raise ReleaseError("Proxy tag changed after asset preparation")
        release = github.release(proxy_tag)
        if release is None or release["draft"] or release["prerelease"]:
            raise ReleaseError("Proxy tag exists without a published stable proxy release. Recover its original assets before retrying.")
        ensure_assets(github, release, directory)
    else:
        if existing is not None:
            raise ReleaseError("Proxy tag appeared after preparation. Reconcile it before retrying; no assets were overwritten.")
        subprocess.run(["git", "tag", "-a", proxy_tag, source, "-m", f"Release {proxy_tag}"], check=True)
        subprocess.run(["git", "push", "origin", f"refs/tags/{proxy_tag}"], check=True)
        release = github.request("POST", "releases", {
            "tag_name": proxy_tag, "name": f"Proxy {version}", "draft": True,
            "body": f"Linux amd64 proxy {version}.\n\nSource commit: `{source}`.\n\nThese assets are reused unchanged by Go worker releases selecting this proxy version.",
            "make_latest": "false",
        })
        ensure_assets(github, release, directory)
        github.request("PATCH", f"releases/{release['id']}", {"draft": False, "make_latest": "false"})
        canonical = github.release(proxy_tag)
        if remote_commit(proxy_tag) != source or canonical is None or canonical["draft"] or canonical["prerelease"]:
            raise ReleaseError("Proxy tag/release publication could not be verified; reconcile before retrying")
        ensure_assets(github, canonical, directory)

    release = github.release(root_tag)
    prerelease = "-" in root_tag
    if release is None:
        previous = github.request("GET", "releases/latest", missing=True)
        notes_request = {"tag_name": root_tag}
        if previous is not None:
            notes_request["previous_tag_name"] = previous["tag_name"]
        notes = github.request("POST", "releases/generate-notes", notes_request)["body"]
        release = github.request("POST", "releases", {
            "tag_name": root_tag, "name": root_tag, "draft": True, "prerelease": prerelease,
            "body": f"{notes}\n\n## Proxy\n\nIncludes proxy **{version}**, built from `{source}` and preserved at `{proxy_tag}`.",
            "make_latest": "false",
        })
    if release["prerelease"] != prerelease:
        raise ReleaseError("Existing release has unexpected prerelease status; reconcile before publishing")
    ensure_assets(github, release, directory)
    published_now = release["draft"]
    if published_now:
        release = github.request("PATCH", f"releases/{release['id']}", {
            "draft": False, "make_latest": "false" if prerelease else "true",
        })
    verified = github.release(root_tag)
    if verified is None or verified["draft"] or verified["prerelease"] != prerelease:
        raise ReleaseError("GitHub Release publication could not be verified; reconcile before retrying")
    ensure_assets(github, verified, directory)
    if published_now and not prerelease:
        latest = github.request("GET", "releases/latest")
        if latest["tag_name"] != root_tag:
            raise ReleaseError("Release was published but latest does not match; reconcile before retrying")
    print(f"Published {verified['html_url']} with proxy {version} from {source}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "publish"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--root-tag", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    action = prepare if args.action == "prepare" else publish
    action(GitHub(), args.directory, args.root_tag, args.commit)


if __name__ == "__main__":
    try:
        main()
    except ReleaseError as error:
        print(f"Release failed: {error}", file=sys.stderr)
        sys.exit(1)
