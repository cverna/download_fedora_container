import os
import argparse
import sys
import subprocess
import shutil
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed
import tarfile
import json


def download_file(url, local_filename):
    print(f"Starting download of {local_filename}...")
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    subprocess.run(["curl", "-s", "-L", url, "-o", local_filename], check=True)
    return local_filename


def download_artifacts_for_architecture(client, base_url, architecture):
    arch_url = urljoin(base_url, architecture + "/images/")
    response = client.get(arch_url)
    response.raise_for_status()
    text = response.text
    soup = BeautifulSoup(text, "html.parser")
    file_urls = [
        (
            urljoin(arch_url, link.get("href")),
            os.path.join(architecture, link.get("href")),
        )
        for link in soup.find_all("a")
        if link.get("href").endswith(".tar.xz") and ("Base" in link.get("href") and ("Minimal" in link.get("href") if mini else "Generic" in link.get("href")))
    ]
    return file_urls


def main(version, mini):
    version_url_part = version.capitalize() if version.lower() == "rawhide" else version
    base_url = f"https://kojipkgs.fedoraproject.org/compose/{version}/latest-Fedora-{version_url_part}/compose/Container/"
    # architectures = ["aarch64", "ppc64le", "s390x", "x86_64"]
    architectures = ["x86_64"]
    with httpx.Client() as client:
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = {}
            for arch in architectures:
                file_urls = download_artifacts_for_architecture(client, base_url, arch)
                for file_url, filename in file_urls:
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    future = executor.submit(download_file, file_url, filename)
                    future_to_url[future] = filename
            for future in as_completed(future_to_url):
                filename = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f"{filename} generated an exception: {exc}")
                else:
                    print(f"Downloaded {data}")
                    decompress_artifact(filename)


def process_artifact(extracted_path):
    index_path = os.path.join(extracted_path, "index.json")
    with open(index_path, "r") as index_file:
        index_data = json.load(index_file)
        digest = index_data["manifests"][0]["digest"].split(":")[1]
        manifest_path = os.path.join(extracted_path, "blobs", "sha256", digest)
        with open(manifest_path, "r") as manifest_file:
            manifest_data = json.load(manifest_file)
            layers_digest = manifest_data["layers"][0]["digest"].split(":")[1]
            layer_path = os.path.join(extracted_path, "blobs", "sha256", layers_digest)
            shutil.copy(layer_path, "layer.tar")
            print(f"Copied layer blob to 'layer.tar' in the current directory.")


def decompress_artifact(artifact_path):
    if artifact_path.endswith(".tar.xz"):
        print(f"Decompressing {artifact_path}...")
        # Decompress the .xz file
        subprocess.run(["xz", "-d", artifact_path], check=True)
        # Extract the .tar file
        tar_path = artifact_path.rstrip(".xz")
        with tarfile.open(tar_path) as tar:
            tar.extractall(path=os.path.dirname(tar_path))
        os.remove(tar_path)
        print(f"Decompressed and extracted {artifact_path}")
        # Ensure we pass the directory path without the file extension
        decompressed_dir = os.path.split(artifact_path)[0]
        print(f"decompressed_dir {decompressed_dir}")
        process_artifact(decompressed_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Fedora container artifacts.")
    parser.add_argument("version", help="The version of Fedora artifacts to download.")
    parser.add_argument("--mini", action="store_true", help="Download only the minimal base artifact.")
    args = parser.parse_args()
    
    main(args.version, args.mini)
