# KV Schema Extraction

A tool for **schema extraction in key-value databases**, focusing on Redis instances. It analyzes stored keys and values, detects structures such as hierarchical keys (flattened key pattern) and semi-structured data (JSON, Hash, List, Set), and generates a DDL representation of the inferred schema.

## Requirements

- Python 3.9+
- Redis (running locally or via Docker)

## Setting Up the Environment

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

## Running the Project

Once the environment is set up, run:

```bash
python main.py
```

The tool will connect to a Redis instance, extract key-value pairs, detect key and value types, and generate an inferred schema.

You can modify the connection parameters (host, port) in the config.ini file.

## Running Redis with Docker

If Redis is not installed locally, you can run it in a container:

```bash
docker run -d -p 6379:6379 --name redis_kvschema redis/redis-stack:latest
```

> The tool connects by default to `localhost` on port `6379`. Make sure Redis is accessible.

## Output Structure

Identified schemas are exported as `output_schema.sql` file in the project folder.

## Synthetic Data Generator
The repository includes a tool named `synthetic_data_generator.py` that generates flattened key names and example values to be inserted into a Redis key-value database based on a simplified DDL input.

### How it works
1. **Prepare a simplified DDL file**
The DDL file should contain basic table declarations with attributes and data types, including primary keys (simple or composite) and foreign keys.

#### Rules and conventions for the DDL file:
- **Working data types**: `INTEGER`, `TEXT`, `DATE`, `TIME`, `DATETIME`, `BOOLEAN`, `DECIMAL`
- **Junction tables** (used for many-to-many relationships) must:
    - Have a **composite primary key**
    - Will result in **aggregate array keys** in the output
- **One-to-one relationships** must be expressed by placing a `UNIQUE` constraint on the foreign key
- All other tables must have a **primary key named in the format**: `TableName_id`
- Foreign keys must be declared using standard SQL syntax:
    ```sql
     FOREIGN KEY (column_name) REFERENCES OtherTable(column)
    ```

2. **Run the generator**  
Execute the script and provide the DDL file name when prompted.
You'll then be asked to choose how to handle junction table relationships (i.e., which related table should receive the embedded attributes).
The program will output a list of `SET` commands with keys in **flattened format** and sample values, exported as `generated_keys.txt` to the project folder.

3. **Insert into Redis**  
You can insert the generated data into Redis using the `redis-cli`:

```bash
redis-cli < generated_keys.txt
```

> An example DDL file named `example.sql` is included in the repository for testing purposes.