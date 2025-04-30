import re

class Table:
    def __init__(self, name):
        self.name = name
        self.columns = {}
        self.primary_keys = []
        self.foreign_keys = []
        self.unique_cols = set()

def parse_ddl(ddl):
    tables = {}
    pattern = re.compile(r"CREATE TABLE (\w+) \((.*?)\);", re.DOTALL | re.IGNORECASE)
    matches = pattern.findall(ddl)

    for table_name, body in matches:
        table = Table(table_name)
        lines = [l.strip().strip(',') for l in body.splitlines() if l.strip()]

        for line in lines:
            if line.upper().startswith("UNIQUE"):
                unique_match = re.search(r"\((.*?)\)", line)
                if unique_match:
                    for col in unique_match.group(1).split(","):
                        table.unique_cols.add(col.strip())
            elif line.upper().startswith("PRIMARY KEY"):
                pk_match = re.search(r"\((.*?)\)", line)
                if pk_match:
                    table.primary_keys.extend([col.strip() for col in pk_match.group(1).split(",")])
            elif line.upper().startswith("FOREIGN KEY"):
                fk_match = re.search(r"FOREIGN KEY \((.*?)\) REFERENCES (\w+)\((.*?)\)", line, re.IGNORECASE)
                if fk_match:
                    col, ref_table, ref_col = map(str.strip, fk_match.groups())
                    table.foreign_keys.append((col, ref_table, ref_col))
            else:
                parts = line.split()
                col_name = parts[0]
                col_type = ' '.join(parts[1:])
                table.columns[col_name] = col_type.upper()
                if 'UNIQUE' in col_type.upper():
                    table.unique_cols.add(col_name)
                if 'PRIMARY KEY' in col_type.upper():
                    table.primary_keys.append(col_name)
        tables[table_name] = table

    return tables

def prompt_user_junction(junction, ref_entities):
    print(f"\n[JUNCTION] Table '{junction}' between: {', '.join(ref_entities)}")
    print("Which table should receive the attributes?")
    for i, ent in enumerate(ref_entities):
        print(f" {i+1} - {ent}")
    while True:
        inp = input(f"Choose (1-{len(ref_entities)}): ").strip()
        if inp.isdigit() and 1 <= int(inp) <= len(ref_entities):
            return ref_entities[int(inp) - 1]

def is_junction_table(table):
    return len(table.primary_keys) > 1 and all(
        any(fk[0] == pk for fk in table.foreign_keys) for pk in table.primary_keys
    )

def get_sample_value(dtype):
    dtype = dtype.upper()
    if "INT" in dtype:
        return "999"
    elif "TEXT" in dtype:
        return "'Sample text'"
    elif "DATE" in dtype:
        return "'2000-01-01'"
    elif "TIME" in dtype:
        return "'12:00:00'"
    elif "TIMESTAMP" in dtype:
        return "'2000-01-01T12:00:00'"
    elif "BOOLEAN" in dtype:
        return "true"
    elif "DECIMAL" in dtype or "NUMERIC" in dtype or "FLOAT" in dtype:
        return "3.14"
    elif dtype == "REF":
        return "999"
    elif dtype == "REF_LIST":
        return "[999]"
    else:
        return "'sample'"

def generate_keys(tables):
    keys = []
    skip_base = set()

    embedded_targets = set()
    for tname, table in tables.items():
        if not is_junction_table(table):
            for fk_col, ref_table, _ in table.foreign_keys:
                if fk_col in table.unique_cols:
                    embedded_targets.add(ref_table)

    for tname, table in tables.items():
        if is_junction_table(table):
            ref_entities = list({fk[1] for fk in table.foreign_keys})
            owner = prompt_user_junction(tname, ref_entities)
            attrs = [a for a in table.columns if a not in table.primary_keys]
            if attrs:
                for attr in attrs:
                    keys.append((f"{owner}:id:{tname}[].{attr}", table.columns[attr]))
                for fk_col, ref_table, _ in table.foreign_keys:
                    if ref_table != owner:
                        keys.append((f"{owner}:id:{tname}[].{fk_col}", tables[tname].columns[fk_col]))
            else:
                keys.append((f"{owner}:id:{tname}", "REF_LIST"))
            skip_base.add(tname)

    for tname, table in tables.items():
        if tname in skip_base or tname in embedded_targets:
            continue

        base = f"{tname}:id"
        for attr in table.columns:
            if attr not in table.primary_keys and all(attr != fk[0] for fk in table.foreign_keys):
                keys.append((f"{base}:{attr}", table.columns[attr]))

        for fk_col, ref_table, _ in table.foreign_keys:
            if fk_col in table.unique_cols:
                for attr in tables[ref_table].columns:
                    if attr not in tables[ref_table].primary_keys:
                        keys.append((f"{base}:{ref_table}.{attr}", tables[ref_table].columns[attr]))
            else:
                keys.append((f"{base}:{fk_col}", "REF"))

    return sorted(keys)

def main():
    print("\nFlattened Object-Key Pattern Generator\n" + "=" * 40)
    filename = input("Enter DDL filename (e.g., example.sql): ").strip()
    try:
        with open(filename, 'r') as f:
            ddl = f.read()
    except FileNotFoundError:
        print("File not found.")
        return

    tables = parse_ddl(ddl)
    if not tables:
        print("No tables found.")
        return

    print(f"\nParsed {len(tables)} tables: {', '.join(tables)}")
    kv_keys = generate_keys(tables)

    with open('generated_keys.txt', 'w') as out:
        for key, dtype in kv_keys:
            redis_key = key.replace(":id:", ":999:").replace("[]", "[0]")
            value = get_sample_value(dtype)
            command = f"SET {redis_key} {value}"
            print(command)
            out.write(command + "\n")

if __name__ == "__main__":
    main()
