import os
import sys
import subprocess
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import aiohttp
import asyncio

async def download_file(url, local_filename):
    print(f'Starting download of {local_filename}...')
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    subprocess.run(['curl', '-L', url, '-o', local_filename], check=True)
    return local_filename

async def download_artifacts_for_architecture(session, base_url, architecture):
    arch_url = urljoin(base_url, architecture + '/images/')
    async with session.get(arch_url) as response:
        response.raise_for_status()
        text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
    file_urls = [(urljoin(arch_url, link.get('href')), os.path.join(architecture, link.get('href')))
                 for link in soup.find_all('a') if link.get('href').endswith('.tar.xz')]
    return file_urls

# This is the new location for the main_async function
async def main_async(version):
    base_url = f'https://kojipkgs.fedoraproject.org/compose/{version}/latest-Fedora-{version}/compose/Container/'
    architectures = ['aarch64', 'ppc64le', 's390x', 'x86_64']
    async with aiohttp.ClientSession() as session:
        tasks = []
        for arch in architectures:
            file_urls = await download_artifacts_for_architecture(session, base_url, arch)
            for file_url, filename in file_urls:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                task = asyncio.ensure_future(download_file(file_url, filename))
                tasks.append(task)
        completed, pending = await asyncio.wait(tasks)
        for task in completed:
            print(f'Downloaded {task.result()}')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python download_artifacts.py <version>")
        sys.exit(1)
    version = sys.argv[1]
    asyncio.run(main_async(version))
