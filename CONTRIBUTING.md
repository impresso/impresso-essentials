## Development Setup

To set up a development environment and install an editable version of the `impresso_essentials` package, follow these steps:

1. **Create a virtual environment**:

   ```sh
   python3 -m venv venv

   ```

2. **Activate the virtual environment**:

   - On Windows:
     ```sh
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```sh
     source venv/bin/activate
     ```

3. **Install the package in editable mode**:

   ```sh
   pip install -e .
   ```

4. **Run the tests**:
   ```sh
   pytest
   ```

This will discover and run all the tests in the tests directory.
