# Docker for Library Project

## Quick summary ✅
This project-level Dockerfile builds a Python image that contains the repository and its Python dependencies so you can run data cleaning, save to SQLite, or run tests in a reproducible container.

---

## Build the image

From the project root (where `Dockerfile` is), run:

```
docker build -t library-system:latest .
```

What happens during build:
- Docker pulls `python:3.12-slim` as the base image
- system packages (minimal build tools) are installed to support building wheels when needed
- `requirements.txt` is copied into the image and `pip install -r requirements.txt` installs dependencies (pandas, numpy, SQLAlchemy, pytest)
- the entire project is copied into `/app`

---

## Run the cleaning script inside the container

To run the cleaning script using the files included in the image (no bind mounts):

```
docker run --rm library-system:latest python library_data_cleaning.py
```

If you want the container to read/write files from/to your host (recommended so outputs persist), mount the project directory as a volume. Example (POSIX shell shown):

```
docker run --rm -v "$(pwd)":/app library-system:latest python library_data_cleaning.py --books-output cleaned_books.csv --customers-output cleaned_customers.csv
```

On Windows PowerShell you can use (example):

```
docker run --rm -v ${PWD}:/app library-system:latest python library_data_cleaning.py --books-output cleaned_books.csv --customers-output cleaned_customers.csv
```

Notes:
- Default CSV inputs in the script are `03_Library Systembook.csv` and `03_Library SystemCustomers.csv` (in the project root)
- When you bind-mount the project folder you can see the output files on your host filesystem after the container finishes.

---

## Run tests in the container

You can run the project's test suite inside the image (pytest):

```
docker run --rm library-system:latest pytest -q
```

This ensures tests run in a controlled environment that matches the installed dependencies.

---

## How the Dockerfile maps to the project 🔧
- `WORKDIR /app` sets the working directory
- `COPY . /app` puts your code and CSVs into the image (unless excluded by `.dockerignore`)
- `pip install -r requirements.txt` installs Python packages needed to run scripts and tests
- `CMD [...]` defines the default command that runs when the container starts; you can override it when running the container

---

If you want, I can:
- add a `docker-compose.yml` to define an easier local workflow (volumes/commands), or
- make a smaller production image that only contains runtime wheels (slim build) and strips build tools.

Tell me which next step you'd like. 😊

---

**Persisting outputs: mounting volumes**

To make the container "remember" files after it stops, mount a directory from your host into the container (a bind mount) or use a Docker named volume. Mounting the project folder into `/app` is the simplest approach — files created or modified inside `/app` will be written to your host filesystem and remain after the container exits.

- Bind-mount current project directory (Linux / macOS):

```bash
docker run --rm -v "$(pwd)":/app library-system:latest \
	python library_data_cleaning.py --books-output cleaned_books.csv --customers-output cleaned_customers.csv
```

- Bind-mount current project directory (Windows PowerShell):

```powershell
docker run --rm -v ${PWD}:/app library-system:latest \
	python library_data_cleaning.py --books-output cleaned_books.csv --customers-output cleaned_customers.csv
```

- Use a named volume (data persists but is managed by Docker):

```bash
docker run --rm -v library-data:/app/library_data library-system:latest \
	python library_data_cleaning.py --books-input /app/library_data/03_Library\ Systembook.csv --books-output /app/library_data/cleaned_books.csv
```

Notes and tips:
- If your script writes an SQLite file (e.g. `library_system.db`), mounting the host path ensures the DB file is accessible after the container stops.
- On Windows, path quoting and permissions can be different; use PowerShell `${PWD}` or provide the full Windows path (for example: `-v "C:\\path\\to\\project":/app`).
- Bind mounts map ownership to the host user (or container user) — if you see permission issues, adjust the mount path or file permissions on the host.
- When developing, bind mounts are handy because you can edit files on the host and run them inside the container immediately.

If you'd like, I can add a `docker-compose.yml` that defines a named volume and ready-to-run service examples for development and testing.

---

**Using docker-compose (recommended for dev)**

A `docker-compose.yml` is included to simplify running the project during development. It builds the image, mounts the project directory so your edits are live in the container, and provides a named volume for persistent data such as `library_system.db` or cleaned CSV outputs.

Create and start the service (builds image if needed):

```bash
docker compose up --build -d
```

Run a one-off command (for example to run the cleaning script and keep output visible):

```bash
docker compose run --rm app python library_data_cleaning.py --books-output cleaned_books.csv --customers-output cleaned_customers.csv
```

Bring down services (volumes remain unless you remove them):

```bash
docker compose down
```

Remove the named volume if you want to wipe persistent data:

```bash
docker volume rm M5-20260106_library-data
```

Notes:
- `docker compose` is the newer command (recommended). Older installations may use `docker-compose` (hyphenated).
- The provided `docker-compose.yml` mounts the current project directory into `/app` and defines a named volume `library-data` mounted at `/app/library_data` inside the container.
- Place any input CSVs you want the container to read into the project folder, or copy them into the named volume path if you prefer Docker-managed storage.

If you'd like, I can tweak the compose file to include a `tests` service that runs the test-suite automatically or add convenience `Makefile` targets.
