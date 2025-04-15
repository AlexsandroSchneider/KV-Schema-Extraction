## 🔍 KV-Schema-Extraction

A tool for **schema extraction in key-value databases**, focusing on Redis instances. It analyzes stored keys and values, detects structures such as hierarchical keys (flattened key pattern) and semi-structured data (e.g., JSON), and generates a DDL representation of the inferred schema.

## 🚀 Requirements

- Python 3.9+
- Redis (running locally or via Docker)

## ⚙️ Setting Up the Environment

1. **Clone the repository**:

```bash
git clone https://github.com/AlexsandroSchneider/KV-Schema-Extraction.git
cd KV-Schema-Extraction
```

2. **Create a virtual environment**:

```bash
python -m venv venv
```

3. **Activate the virtual environment**:

- **Windows**:
```bash
venv\Scripts\activate
```
- **Linux/macOS**:
```bash
source venv/bin/activate
```

4. **Install the dependencies**:

```bash
pip install -r requirements.txt
```

## 🧪 Running the Project

Once the environment is set up, run:

```bash
python main.py
```

The tool will connect to a Redis instance, extract key-value pairs, detect key and value types, and generate an inferred schema.

You can modify the connection parameters (host, port) in the config.ini file.

## 🐳 Running Redis with Docker

If Redis is not installed locally, you can run it in a container:

```bash
docker run -d -p 6379:6379 --name redis_kvschema redis/redis-stack:latest
```

> The tool connects by default to `localhost` on port `6379`. Make sure Redis is accessible.

## 📂 Output Structure

Identified schemas are exported as `schema.sql` file in the project folder.