import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

def download_artifacts_for_architecture(base_url, architecture):
    arch_url = urljoin(base_url, architecture + '/images/')
    response = requests.get(arch_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    for link in soup.find_all('a'):
        file_url = urljoin(arch_url, link.get('href'))
        if file_url.endswith('.tar.xz'):
            filename = os.path.join(architecture, link.get('href'))
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            print(f'Downloading {file_url}...')
            download_file(file_url, filename)

def main():
    base_url = 'https://kojipkgs.fedoraproject.org/compose/40/latest-Fedora-40/compose/Container/'
    architectures = ['aarch64', 'ppc64le', 's390x', 'x86_64']
    for arch in architectures:
        download_artifacts_for_architecture(base_url, arch)

if __name__ == '__main__':
    main()
