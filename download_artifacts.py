import argparse
import json
import lzma
import os
import shutil
import subprocess
import sys
import tarfile
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader


def download_file(client, url, output_path):
    local_filename = os.path.basename(output_path)
    print(f"Starting download of {url}...")
    with client.stream("GET", url) as response:
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_bytes():
                f.write(chunk)
    return output_path


def download_artifacts_for_architecture(client, base_url, architecture):
    response = client.get(base_url)
    response.raise_for_status()
    text = response.text
    soup = BeautifulSoup(text, "html.parser")
    file_urls = [
        (
            urljoin(base_url, link.get("href")),
            os.path.join(architecture, link.get("href")),
        )
        for link in soup.find_all("a")
        if link.get("href").endswith(".tar.xz") and architecture in link.get("href")
    ]
    return file_urls


def get_digest_from_index(index_path):
    with open(index_path, "r") as index_file:
        index_data = json.load(index_file)
    return index_data["manifests"][0]["digest"].split(":")[1]

def get_current_date():
    return date.today().strftime("%Y%m%d")

def get_tar_name():
    current_date = get_current_date()
    return f"fedora-{current_date}.tar"

def copy_layer_blob_to_tar(extracted_path, digest, tar_name):
    manifest_path = os.path.join(extracted_path, "blobs", "sha256", digest)
    with open(manifest_path, "r") as manifest_file:
        manifest_data = json.load(manifest_file)
    layers_digest = manifest_data["layers"][0]["digest"].split(":")[1]
    layer_path = os.path.join(extracted_path, "blobs", "sha256", layers_digest)
    shutil.copy(layer_path, os.path.join(extracted_path, tar_name))
    print(f"Copied layer blob to '{tar_name}' in {extracted_path}.")


def delete_extraction_artifacts(extracted_path):
    """Delete the blobs/, index.json, and oci-layout from the extracted_path."""
    blobs_path = os.path.join(extracted_path, "blobs")
    index_json_path = os.path.join(extracted_path, "index.json")
    oci_layout_path = os.path.join(extracted_path, "oci-layout")
    shutil.rmtree(blobs_path, ignore_errors=True)
    os.remove(index_json_path)
    os.remove(oci_layout_path)
    print(f"Deleted blobs/, index.json, and oci-layout from {extracted_path}.")


def process_artifact(extracted_path, version):
    digest = get_digest_from_index(os.path.join(extracted_path, "index.json"))
    tar_name = get_tar_name()
    copy_layer_blob_to_tar(extracted_path, digest, tar_name)

    # Render Dockerfile from template
    env = Environment(
        loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
    )
    template = env.get_template("Dockerfile")
    rendered_version = "rawhide" if version.lower() == "rawhide" else f"f{version}"
    dockerfile_content = template.render(version=rendered_version, tar_name=tar_name)
    dockerfile_path = os.path.join(extracted_path, "Dockerfile")
    with open(dockerfile_path, "w") as dockerfile:
        dockerfile.write(dockerfile_content)
    print(f"Rendered Dockerfile in {extracted_path}.")
    delete_extraction_artifacts(extracted_path)


def decompress_artifact(artifact_path, version):
    if artifact_path.endswith(".tar.xz"):
        print(f"Decompressing {artifact_path}...")
        # Decompress the .xz file
        # Decompress the .xz file using lzma
        tar_path = artifact_path.rstrip(".xz")
        with lzma.open(artifact_path, "rb") as compressed, open(tar_path, "wb") as f:
            shutil.copyfileobj(compressed, f)
        # Extract the .tar file
        with tarfile.open(tar_path) as tar:
            tar.extractall(path=os.path.dirname(tar_path))
        os.remove(artifact_path)
        print(f"Decompressed and extracted {tar_path}")
        os.remove(tar_path)
        print(f"Deleted {tar_path}")
        # Ensure we pass the directory path without the file extension
        decompressed_dir = os.path.split(tar_path)[0]
        process_artifact(decompressed_dir, version)


def main(version, output_dir, workers, branched, rawhide):
    if rawhide:
        base_url = f"https://kojipkgs.fedoraproject.org/packages/Fedora-Container-Base-Generic/Rawhide/{get_current_date()}.n.0/images/"
    elif branched:
        base_url = f"https://kojipkgs.fedoraproject.org/packages/Fedora-Container-Base-Generic/{version}/{get_current_date()}.n.0/images/"
    else:
        base_url = f"https://kojipkgs.fedoraproject.org/packages/Fedora-Container-Base-Generic/{version}/{get_current_date()}.0/images/"
    architectures = ["aarch64", "ppc64le", "s390x", "x86_64"]
    # architectures = ["x86_64"]
    with httpx.Client(follow_redirects=True, timeout=None) as client:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_url = {}
            for arch in architectures:
                file_urls = download_artifacts_for_architecture(
                    client, base_url, arch
                )
                for file_url, filename in file_urls:
                    # Ensure the output directory structure is created
                    output_path = os.path.join(output_dir, filename)
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    future = executor.submit(
                        download_file, client, file_url, output_path
                    )
                    future_to_url[future] = filename
            for future in as_completed(future_to_url):
                output_path = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f"{output_path} generated an exception: {exc}")
                else:
                    print(f"Downloaded to {data}")
                    decompress_artifact(data, version)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Fedora container artifacts.")
    parser.add_argument("version", help="The version of Fedora artifacts to download.")
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory where artifacts will be downloaded and extracted.",
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker threads for downloading (default: 1)"
    )
    parser.add_argument(
        "--branched", action="store_true", help="Use 'branched' in the URL instead of the version number."
    )
    parser.add_argument(
        "--rawhide", action="store_true", help="Treat the version as Rawhide."
    )
    args = parser.parse_args()

    main(args.version, args.output_dir, args.workers, args.branched, args.rawhide)
