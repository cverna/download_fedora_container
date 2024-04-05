# Fedora Artifact Downloader

## Setting Up the Python Virtual Environment

To run the Fedora Artifact Downloader, you should set up a Python virtual environment. This ensures that the dependencies required by the project do not interfere with other Python projects or system-wide packages.

1. Ensure you have Python installed on your system. Python 3.6 or higher is required.
2. Install `virtualenv` if you haven't already:

   ```bash
   pip install virtualenv
   ```

3. Navigate to the project directory and create a virtual environment:

   ```bash
   virtualenv venv
   ```

4. Activate the virtual environment:

   - On Windows:

     ```bash
     .\venv\Scripts\activate
     ```

   - On macOS and Linux:

     ```bash
     source venv/bin/activate
     ```

5. Your command prompt should now reflect that you are working inside the 'venv' environment.

## Installing Dependencies

With the virtual environment activated, install the project dependencies by running:

```bash
pip install -r requirements.txt
```

## Running the Application

To run the Fedora Artifact Downloader, use the following command:

```bash
python download_artifacts.py <version> [--output-dir <path>] [--mini]
```

- `<version>`: The version of Fedora artifacts to download (e.g., 34, 35, rawhide).
- `--output-dir <path>`: (Optional) The directory where artifacts will be downloaded and extracted. Defaults to the current directory.
- `--mini`: (Optional) If set, only the minimal base artifact will be downloaded.

For example, to download Fedora 35 artifacts to a specific directory, you would run:

```bash
python download_artifacts.py 35 --output-dir /path/to/output
```

To deactivate the virtual environment when you're done, simply run:

```bash
deactivate
```
