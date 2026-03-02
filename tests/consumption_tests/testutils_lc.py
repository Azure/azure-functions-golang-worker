# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from io import BytesIO
from pathlib import Path
from typing import Dict
from urllib.request import urlopen
from zipfile import ZipFile

import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import padding


TESTS_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT = TESTS_ROOT.parent
_FUNCTION_APP_ZIPS_DIR = TESTS_ROOT / 'consumption_tests' / 'function_app_zips'

# Flex Consumption Testing Constants
_DOCKER_PATH = "DOCKER_PATH"
_DOCKER_DEFAULT_PATH = "docker"
_OS_TYPE = "bookworm" 
_MESH_IMAGE_REPO = f"mcr.microsoft.com/azure-functions/{_OS_TYPE}/flexconsumption"

class FlexConsumptionWebHostController:
    """A controller for spawning mesh Docker container and apply multiple
    test cases on it.
    """

    _docker_cmd = os.getenv(_DOCKER_PATH, _DOCKER_DEFAULT_PATH)
    _ports: Dict[str, str] = {}  # { uuid: port }
    _mesh_images: Dict[str, str] = {}  # { host version: image tag }

    def __init__(self, host_version: str, go_version: str = "1.24"):
        """Initialize a new container for Go worker testing.
        """
        self._uuid = str(uuid.uuid4())
        self._host_version = host_version  # "4"
        self._go_version = go_version  # "1.24"

    @property
    def url(self) -> str:
        if self._uuid not in self._ports:
            raise RuntimeError(f'Failed to assign container {self._name} since'
                               ' it is not spawned')

        return f'http://localhost:{self._ports[self._uuid]}'

    def assign_container(self, env: Dict[str, str] = {}):
        """Make a POST request to /admin/instance/assign to specialize the
        container.
        """
        url = f'http://localhost:{self._ports[self._uuid]}'

        # Add compulsory fields in specialization context
        env["FUNCTIONS_EXTENSION_VERSION"] = f"~{self._host_version}"
        env["FUNCTIONS_WORKER_RUNTIME"] = "golang"
        env["FUNCTIONS_WORKER_RUNTIME_VERSION"] = self._go_version
        env["WEBSITE_SITE_NAME"] = self._uuid
        env["WEBSITE_POD_NAME"] = self._uuid

        max_retries = 10
        for i in range(max_retries):
            try:
                ping_req = requests.Request(method="GET", url=f"{url}/admin/host/ping")
                ping_response = self.send_request(ping_req)
                if ping_response.ok:
                    break
            except Exception as e:
                pass
            time.sleep(1)
        else:
            raise RuntimeError(f'Container {self._uuid} did not become ready in time')

        # Flex/Legion host does not download app content during assign (it's a
        # no-op).  In local Docker tests there is no Legion infrastructure, so
        # we must manually mount the content BEFORE the assign call so the
        # host can discover functions when it specializes.
        pkg_name = env.get("SCM_RUN_FROM_PACKAGE") or env.get(
            "WEBSITE_RUN_FROM_PACKAGE"
        )
        if pkg_name:
            local_zip = _FUNCTION_APP_ZIPS_DIR / pkg_name
            if not local_zip.exists():
                raise RuntimeError(
                    f"Local function app zip not found: {local_zip}"
                )
            self._mount_package_in_container(str(local_zip))

        # Send the specialization context via a POST request
        req = requests.Request(
            method="POST",
            url=f"{url}/admin/instance/assign",
            data=json.dumps({
                "encryptedContext": self._get_site_encrypted_context(
                    self._uuid, env
                )
            })
        )
        response = self.send_request(req)
        if not response.ok:
            stdout = self.get_container_logs()
            raise RuntimeError(f'Failed to specialize container {self._uuid}'
                               f' at {url} (status {response.status_code}).'
                               f' stdout: {stdout}')

    def _mount_package_in_container(self, local_path: str):
        """Copy a local function app package into the container and
        mount/extract it at /home/site/wwwroot.

        Supports both regular zip files (PK magic) and SquashFS images
        (hsqs magic) which Azure Functions uses for Flex Consumption.
        """
        with open(local_path, "rb") as f:
            magic = f.read(4)

        is_squashfs = (magic == b'hsqs')
        is_zip = (magic[:2] == b'PK')

        if not is_squashfs and not is_zip:
            raise RuntimeError(
                f"{local_path} is neither a zip nor a squashfs image. "
                f"First 4 bytes: {magic}"
            )

        container_pkg = "/tmp/app.sqsh" if is_squashfs else "/tmp/app.zip"

        # Copy the package into the container
        subprocess.run(
            [self._docker_cmd, "cp", local_path,
             f"{self._uuid}:{container_pkg}"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        if is_squashfs:
            # Mount squashfs image at /home/site/wwwroot
            subprocess.run(
                [self._docker_cmd, "exec", self._uuid,
                 "bash", "-c",
                 "mkdir -p /home/site/wwwroot "
                 f"&& mount -t squashfs -o loop {container_pkg} "
                 "/home/site/wwwroot"],
                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
        else:
            # Extract zip locally and copy contents into container
            # (avoids needing unzip/python inside the container)
            import tempfile
            with tempfile.TemporaryDirectory() as tmp_dir:
                with ZipFile(local_path, 'r') as zf:
                    zf.extractall(tmp_dir)

                # Ensure /home/site/wwwroot exists in container
                subprocess.run(
                    [self._docker_cmd, "exec", self._uuid,
                     "mkdir", "-p", "/home/site/wwwroot"],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )

                # Copy extracted contents into container
                for item in os.listdir(tmp_dir):
                    src = os.path.join(tmp_dir, item)
                    subprocess.run(
                        [self._docker_cmd, "cp", src,
                         f"{self._uuid}:/home/site/wwwroot/{item}"],
                        check=True, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )

                # Make binaries executable
                subprocess.run(
                    [self._docker_cmd, "exec", self._uuid,
                     "chmod", "+x", "/home/site/wwwroot/app"],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )

    def send_request(
            self,
            req: requests.Request,
            ses: requests.Session = None
    ) -> requests.Response:
        """Send a request with authorization token. Return a Response object"""
        session = ses
        if session is None:
            session = requests.Session()

        prepped = session.prepare_request(req)
        prepped.headers['Content-Type'] = 'application/json'

        # For flex consumption, use JWT Bearer token
        jwt_token = self._generate_jwt_token()
        prepped.headers['Authorization'] = f'Bearer {jwt_token}'

        # Add additional headers required by Azure Functions host
        prepped.headers['x-site-deployment-id'] = self._uuid
        prepped.headers['x-ms-client-request-id'] = str(uuid.uuid4())
        prepped.headers['x-ms-request-id'] = str(uuid.uuid4())

        resp = session.send(prepped)
        return resp

    @staticmethod
    def _find_latest_mesh_image() -> str:
        image_tag = f"{_MESH_IMAGE_REPO}:4.1047.100-0-custom1.0"
        return image_tag


    def spawn_container(self,
                        image: str,
                        env: Dict[str, str] = {}) -> int:
        """Create a docker container and record its port."""

        # Paths to Go worker artifacts in the repo
        worker_config_path = PROJECT_ROOT / 'worker.config.json'
        proxy_exe_path = PROJECT_ROOT / 'bin' / 'proxy'
        container_worker_dir = '/azure-functions-host/workers/golang'

        run_cmd = []
        run_cmd.extend([self._docker_cmd, "run", "-p", "0:80", "-d"])
        run_cmd.extend(["--name", self._uuid, "--privileged"])
        run_cmd.extend(["--cap-add", "SYS_ADMIN"])
        run_cmd.extend(["--device", "/dev/fuse"])
        run_cmd.extend(["-e",
                        f"CONTAINER_ENCRYPTION_KEY={os.getenv('_DUMMY_CONT_KEY')}"])
        run_cmd.extend(["-e", "WEBSITE_PLACEHOLDER_MODE=1"])
        run_cmd.extend(["-e", f"WEBSITE_SITE_NAME={self._uuid}"])
        run_cmd.extend(["-e", f"WEBSITE_POD_NAME={self._uuid}"])
        run_cmd.extend(["-e", "WEBSITE_SKU=FlexConsumption"])

        # Mount Go worker proxy and config into the container
        run_cmd.extend([
            "-v",
            f"{worker_config_path}:{container_worker_dir}/worker.config.json"
        ])
        run_cmd.extend([
            "-v",
            f"{proxy_exe_path}:{container_worker_dir}/proxy"
        ])

        for key, value in env.items():
            run_cmd.extend(["-e", f"{key}={value}"])
        run_cmd.append(image)

        run_process = subprocess.run(args=run_cmd,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)

        if run_process.returncode != 0:
            raise RuntimeError('Failed to spawn docker container for'
                               f' {image} with uuid {self._uuid}.'
                               f' stderr: {run_process.stderr}')

        # Make proxy binary executable and symlink it to PATH
        subprocess.run(
            [self._docker_cmd, "exec", self._uuid,
             "bash", "-c",
             f"chmod +x {container_worker_dir}/proxy "
             f"&& ln -sf {container_worker_dir}/proxy /usr/local/bin/proxy"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Wait for three seconds for the port to expose
        time.sleep(3)

        # Acquire the port number of the container
        port_cmd = [self._docker_cmd, "port", self._uuid]
        port_process = subprocess.run(args=port_cmd,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
        if port_process.returncode != 0:
            raise RuntimeError(f'Failed to acquire port for {self._uuid}.'
                               f' stderr: {port_process.stderr}')
        port_number = port_process.stdout.decode().strip('\n').split(':')[-1]

        # Register port number onto the table
        self._ports[self._uuid] = port_number

        # Wait for three seconds for the container to be in ready state
        time.sleep(6)
        return port_number

    def get_container_logs(self) -> str:
        """Get container logs, the first element in tuple is stdout and the
        second element is stderr
        """
        get_logs_cmd = [self._docker_cmd, "logs", self._uuid]
        get_logs_process = subprocess.run(args=get_logs_cmd,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)

        # The `docker logs` command will merge stdout and stderr into stdout
        return get_logs_process.stdout.decode('utf-8')

    def safe_kill_container(self) -> bool:
        """Kill a container by its name. Returns True on success.
        """
        kill_cmd = [self._docker_cmd, "rm", "-f", self._uuid]
        kill_process = subprocess.run(args=kill_cmd, stdout=subprocess.DEVNULL)
        exit_code = kill_process.returncode

        if self._uuid in self._ports:
            del self._ports[self._uuid]
        return exit_code == 0

    @classmethod
    def _get_site_restricted_token(cls) -> str:
        """Get SWT token for site-restricted authentication."""
        exp_ns = int((time.time() + 24 * 60 * 60) * 1000000000)
        token = cls._encrypt_context(os.getenv('_DUMMY_CONT_KEY'), f'exp={exp_ns}')
        return token

    def _generate_jwt_token(self) -> str:
        """Generate JWT token for Flex consumption authentication."""
        try:
            import jwt
        except ImportError as e:
            raise RuntimeError("PyJWT library required. Install with: pip install pyjwt") from e

        exp_time = int(time.time()) + (24 * 60 * 60)
        iat_time = int(time.time())
        site_name = self._uuid
        issuer = f"https://{site_name}.azurewebsites.net"
        
        # Flex Consumption Host validation can be tricky with exact audience matching.
        # Provide a comprehensive list of potential expected audiences.
        audience = [
            issuer,
            f"{issuer}/",
            site_name,
            f"{site_name}.azurewebsites.net",
            f"https://{site_name}.azurewebsites.net",
            f"https://{site_name}.azurewebsites.net/",
            "https://azure-functions-host", 
            "https://localhost",
        ]

        payload = {
            'exp': exp_time,
            'iat': iat_time,
            'nbf': iat_time,
            'iss': issuer,
            'aud': self._uuid,
            'sub': site_name,
        }

        encryption_key_str = os.getenv('_DUMMY_CONT_KEY')
        if not encryption_key_str:
            raise RuntimeError("_DUMMY_CONT_KEY environment variable not set")

        key_bytes = base64.b64decode(encryption_key_str.encode())
        key = key_bytes[:32]
        jwt_token = jwt.encode(payload, key, algorithm='HS256')
        return jwt_token

    @classmethod
    def _get_site_encrypted_context(cls, site_name: str, env: Dict[str, str]) -> str:
        """Get encrypted specialization context."""
        env["WEBSITE_SITE_NAME"] = site_name
        ctx = {"siteId": 1, "siteName": site_name, "environment": env}
        json_ctx = json.dumps(ctx)
        encrypted = cls._encrypt_context(os.getenv('_DUMMY_CONT_KEY'), json_ctx)
        return encrypted

    @classmethod
    def _encrypt_context(cls, encryption_key: str, plain_text: str) -> str:
        """Encrypt context for specialization."""
        encryption_key_bytes = base64.b64decode(encryption_key.encode())
        aes_key = encryption_key_bytes[:32]

        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        plain_text_bytes = padder.update(plain_text.encode()) + padder.finalize()

        iv_bytes = '0123456789abcedf'.encode()
        cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv_bytes), backend=default_backend())
        encryptor = cipher.encryptor()
        encrypted_bytes = encryptor.update(plain_text_bytes) + encryptor.finalize()

        iv_base64 = base64.b64encode(iv_bytes).decode()
        encrypted_base64 = base64.b64encode(encrypted_bytes).decode()

        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(aes_key)
        key_sha256 = digest.finalize()
        key_sha256_base64 = base64.b64encode(key_sha256).decode()

        return f'{iv_base64}.{encrypted_base64}.{key_sha256_base64}'

    def __enter__(self):
        mesh_image = self._find_latest_mesh_image()
        self.spawn_container(image=mesh_image)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logs = self.get_container_logs()
        self.safe_kill_container()
        if traceback:
            print(f'Test failed with container logs: {logs}',
                  file=sys.stderr,
                  flush=True)
