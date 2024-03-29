import os
import sys
import subprocess
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import httpx
from concurrent.futures import ThreadPoolExecutor

def download_file(url, local_filename):
    print(f'Starting download of {local_filename}...')
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    subprocess.run(['curl', '-s', '-L', url, '-o', local_filename], check=True)
    return local_filename

def download_artifacts_for_architecture(client, base_url, architecture):
    arch_url = urljoin(base_url, architecture + '/images/')
    response = client.get(arch_url)
    response.raise_for_status()
    text = response.text
    soup = BeautifulSoup(text, 'html.parser')
    file_urls = [(urljoin(arch_url, link.get('href')), os.path.join(architecture, link.get('href')))
                 for link in soup.find_all('a') if link.get('href').endswith('.tar.xz')]
    return file_urls

# This is the new location for the main_async function
def main(version):
    base_url = f'https://kojipkgs.fedoraproject.org/compose/{version}/latest-Fedora-{version}/compose/Container/'
    architectures = ['aarch64', 'ppc64le', 's390x', 'x86_64']
    with httpx.Client() as client:
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = {}
            for arch in architectures:
                file_urls = download_artifacts_for_architecture(client, base_url, arch)
                for file_url, filename in file_urls:
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    future = executor.submit(download_file, file_url, filename)
                    future_to_url[future] = filename
            for future in concurrent.futures.as_completed(future_to_url):
                filename = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f'{filename} generated an exception: {exc}')
                else:
                    print(f'Downloaded {data}')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python download_artifacts.py <version>")
        sys.exit(1)
    version = sys.argv[1]
    main(version)
