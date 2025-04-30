import re
import yaml
from classes import Table, Entity_Object, Attribute
from thefuzz.fuzz import WRatio
from collections import defaultdict

def infer_data_type(values: list[str]) -> str:
    values_types = defaultdict(int)
    for value in values:
        if re.match(r"^-?\d+$", value):
            values_types["INTEGER"] += 1
        elif re.match(r"^-?\d+([.,]\d+)?$", value):
            values_types["DECIMAL"] += 1
        elif value.lower() in ["true", "false"]:
            values_types["BOOLEAN"] += 1
        elif re.match(r"^\d{4}-\d{2}-\d{2}$", value): ## (YYYY-MM-DD)
            values_types["DATE"] += 1
        elif re.match(r"^\d{2}:\d{2}:\d{2}$", value): ## (YYYY-MM-DD)
            values_types["TIME"] += 1
        elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", value): ## TIMESTAMP (YYYY-MM-DD HH:MM:SS)
            values_types["TIMESTAMP"] += 1
        else:
            values_types["TEXT"] += 1
            
    sorted_types = sorted(values_types.items(), key=lambda k: k[1], reverse=True)
    return sorted_types[0][0] if sorted_types else "TEXT"

def find_relation(name: str, strings: list[str]) -> tuple[int, str]:
    score, table_name = 0, None
    for string in strings:
        ratio = WRatio(name, string)
        if ratio > score:
            score, table_name = ratio, string
    return score, table_name

def process_attributes(attrs: list[Attribute], table: Table, table_names: list[str], threshold: int):
    for k,v in attrs:
        if k and k.lower() != "id": ## Attribute name is not "None" nor "id"
            if table.columns.get(k, False) or k in [k[0] for k in table.foreign_keys]:
                table.add_column(k, v)  ## Attribute | FK in Table, only add new value
            else:
                score, table_name = find_relation(k, table_names) ## Find best match
                if score < threshold:
                    table.add_column(k, v)
                else: # REF
                    table.add_column(k, v)
                    table.add_foreign_key(k, table_name, f"{table_name}_id")

def generate_relational_model(objects: list[Entity_Object], threshold = int):
    tables = defaultdict(lambda: Table(name=None))

    for obj in objects: ## First pass - create entities
        entity_table = tables[obj.name]
        entity_table.name = obj.name

        for aggregate_name in obj.aggregates.keys(): ## 1:1
            aggregate_table = tables[aggregate_name]
            aggregate_table.name = aggregate_name
        
        for agg_arr_name in obj.aggregate_arrays.keys(): ## 1:N, N:N
            agg_arr_table = tables[agg_arr_name]
            agg_arr_table = agg_arr_name
      
    table_names = list(tables.keys()) ## Get all table names for fuzzy matching (identify relationship)

    for obj in objects: ## Second pass - set PKs, FKs, and processes attributes
        entity_name = obj.name

        entity_table = tables[entity_name]
        entity_table.count += 1
        entity_table.add_column(f"{entity_name}_id", 999)  ## PK = _id (U-schema format)
        entity_table.set_primary_key(f"{entity_name}_id")

        ## Add entity attributes
        filtered = [t for t in table_names if t != entity_name] ## Filter to avoid referencing same entity
        process_attributes(obj.attributes, entity_table, filtered, threshold)

        ## Handle 1:1 relationship
        for aggregate_name, attributes in obj.aggregates.items(): ## 1:1

            aggregate_table = tables[aggregate_name]
            aggregate_table.count += 1
            aggregate_table.add_column(f"{aggregate_name}_id", 999) ## PK = _id (U-schema format)
            aggregate_table.set_primary_key(f"{aggregate_name}_id")

            # FK goes to the main entity table, as "Entity HAS aggregate"
            entity_table.add_column(f"{aggregate_name}_id", 999)  ## FK = aggregate.name+ID
            entity_table.add_foreign_key(f"{aggregate_name}_id", aggregate_name, f"{aggregate_name}_id") ## FK on table, REF table, column on REF table

            ## Add aggregate attributes
            filtered = [t for t in table_names if t != aggregate_name]
            process_attributes(attributes, aggregate_table, filtered, threshold)

        ## Handle 1:N, N:N relationships
        for agg_arr_name, agg_arr in obj.aggregate_arrays.items(): ## For each aggregate array in object. Ex: watchedMovies{], favoriteMovies{}
            
            agg_arr_name = agg_arr_name

            filtered = [t for t in table_names if t != agg_arr_name] ## Filters out agg_arr_name from table_names

            score, table_name = find_relation(agg_arr_name, filtered)

            for attributes in agg_arr.values(): ## For each instance in aggregate array. Ex: watchedMovie[0]

                agg_arr_table = tables[agg_arr_name] ## Creates/Updates aggregate array entity table
                agg_arr_table.count += 1
                agg_arr_table.name = agg_arr_name
                agg_arr_table.add_column(f"{entity_name}_id", 999)
                agg_arr_table.add_foreign_key(f"{entity_name}_id", entity_name, f"{entity_name}_id") ## Adds main entity as FK

                if score >= threshold:
                    fk_attrib = max([(WRatio(k, table_name), k, v) for k,v in attributes]) ## Ex: movie_id -> REF to -> Movie

                    if fk_attrib[0] > threshold: ## N:N with entity_ID attribute
                        agg_arr_table.add_column(fk_attrib[1], fk_attrib[2])
                        agg_arr_table.add_foreign_key(fk_attrib[1], table_name, f"{table_name}_id") ## Add identified fk_attrib as FK
                        agg_arr_table.set_primary_key((f"{entity_name}_id", fk_attrib[1])) ## Composite PK (entity_ID, fk_attrib_ID)
                        attributes = [attr for attr in attributes if attr.name != fk_attrib[1]] ## Filters out attributes == fk_attrib

                    else: ## N:N without entity_ID attribute
                        agg_arr_table.add_column(f"{table_name}_id", 999)
                        agg_arr_table.add_foreign_key(f"{table_name}_id", table_name, f"{table_name}_id") # FK on table, REF table, column on REF table
                        agg_arr_table.set_primary_key((f"{entity_name}_id", f"{table_name}_id")) ## Composite PK (entity_ID, table_name_ID)

                else: ## Handle 1:N relationship
                    agg_arr_table.add_column(f"{agg_arr_name}_id", 999)
                    agg_arr_table.set_primary_key(f"{agg_arr_name}_id") ## PK = _id
                
                process_attributes(attributes, agg_arr_table, filtered, threshold)

    ## Infer column data types and nullability
    for table_name, table in tables.items():
        for column in table.columns.values():
            column.data_type = infer_data_type(column.values)
            column.nullable = len(column.values) < table.count ## N° of objects with a column is less than the total n° of objects = nullable(optional)
            column.values.clear() ## Empty the values to reduce memory usage
            #print(f"{table_name}:{column.name}", len(column.values), '/', table.count, column.values) ### DEBUG

    return tables

def generate_sql_schema(tables: defaultdict[str, Table]):
    sql_statements = []
    for table_name, table in tables.items():
        columns = []
        composite_pk = isinstance(table.primary_key, tuple)

        ## Primary key is simple, appears on top of table
        if not composite_pk:
            columns.append(f"{table.primary_key} INTEGER PRIMARY KEY")

        ## Add regular columns
        for column in table.columns.values():
            if column.name != table.primary_key:
                field = f"{column.name} {column.data_type}"
                if not column.nullable:
                    field += " NOT NULL"
                columns.append(field)

        ## Primary key is composite, appears after key columns are created
        if composite_pk:
            columns.append(f"PRIMARY KEY ({table.primary_key[0]}, {table.primary_key[1]})")

        ## Add foreign keys
        for fk in table.foreign_keys:
            columns.append(f"FOREIGN KEY ({fk[0]}) REFERENCES {fk[1]}({fk[2]})")
        
        ## Create SQL statement
        sql_statements.append(f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n);")
    return sql_statements