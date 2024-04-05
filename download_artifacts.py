import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader


def download_file(url, output_path):
    local_filename = os.path.basename(output_path)
    print(f"Starting download of {local_filename}...")
    subprocess.run(["curl", "-s", "-L", url, "-o", output_path], check=True)
    return output_path


def download_artifacts_for_architecture(client, base_url, architecture, mini):
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
        if link.get("href").endswith(".tar.xz")
        and "Base" in link.get("href")
        and (mini or "Minimal" not in link.get("href"))
    ]
    return file_urls


def get_digest_from_index(index_path):
    with open(index_path, "r") as index_file:
        index_data = json.load(index_file)
    return index_data["manifests"][0]["digest"].split(":")[1]


def copy_layer_blob_to_tar(extracted_path, digest):
    manifest_path = os.path.join(extracted_path, "blobs", "sha256", digest)
    with open(manifest_path, "r") as manifest_file:
        manifest_data = json.load(manifest_file)
    layers_digest = manifest_data["layers"][0]["digest"].split(":")[1]
    layer_path = os.path.join(extracted_path, "blobs", "sha256", layers_digest)
    shutil.copy(layer_path, os.path.join(extracted_path, "layer.tar"))
    print(f"Copied layer blob to 'layer.tar' in {extracted_path}.")


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
    copy_layer_blob_to_tar(extracted_path, digest)

    # Render Dockerfile from template
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("Dockerfile")
    rendered_version = "rawhide" if version.lower() == "rawhide" else f"f{version}"
    dockerfile_content = template.render(version=rendered_version)
    dockerfile_path = os.path.join(extracted_path, "Dockerfile")
    with open(dockerfile_path, "w") as dockerfile:
        dockerfile.write(dockerfile_content)
    print(f"Rendered Dockerfile in {extracted_path}.")
    delete_extraction_artifacts(extracted_path)


def decompress_artifact(artifact_path, version):
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
        process_artifact(decompressed_dir, version)


def main(version, mini, output_dir):
    version_url_part = version.capitalize() if version.lower() == "rawhide" else version
    base_url = f"https://kojipkgs.fedoraproject.org/compose/{version}/latest-Fedora-{version_url_part}/compose/Container/"
    #architectures = ["aarch64", "ppc64le", "s390x", "x86_64"]
    architectures = ["x86_64"]
    with httpx.Client() as client:
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = {}
            for arch in architectures:
                file_urls = download_artifacts_for_architecture(
                    client, base_url, arch, mini
                )
                for file_url, filename in file_urls:
                    # Ensure the output directory structure is created
                    output_path = os.path.join(output_dir, filename)
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    future = executor.submit(download_file, file_url, output_path)
                    future_to_url[future] = filename
            for future in as_completed(future_to_url):
                output_path = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f"{output_path} generated an exception: {exc}")
                else:
                    print(f"Downloaded {data}")
                    decompress_artifact(output_path, version)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Fedora container artifacts.")
    parser.add_argument("version", help="The version of Fedora artifacts to download.")
    parser.add_argument("--output-dir", default=".", help="Directory where artifacts will be downloaded and extracted.")
    parser.add_argument("--mini", action="store_true", help="Download only the minimal base artifact.")
    args = parser.parse_args()

    main(args.version, args.mini, args.output_dir)
