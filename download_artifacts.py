import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor

def download_file(url, local_filename):
    print(f'Starting download of {local_filename}...')
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=10 * 1024):
                f.write(chunk)
    return local_filename

def download_artifacts_for_architecture(base_url, architecture):
    arch_url = urljoin(base_url, architecture + '/images/')
    response = requests.get(arch_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    file_urls = [(urljoin(arch_url, link.get('href')), os.path.join(architecture, link.get('href')))
                 for link in soup.find_all('a') if link.get('href').endswith('.tar.xz')]
    return file_urls

def main(version):
    base_url = f'https://kojipkgs.fedoraproject.org/compose/{version}/latest-Fedora-{version}/compose/Container/'
    architectures = ['aarch64', 'ppc64le', 's390x', 'x86_64']
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for arch in architectures:
            file_urls = download_artifacts_for_architecture(base_url, arch)
            for file_url, filename in file_urls:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                futures.append(executor.submit(download_file, file_url, filename))
        for future in futures:
            print(f'Downloaded {future.result()}')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python download_artifacts.py <version>")
        sys.exit(1)
    version = sys.argv[1]
    main(version)
