from collections import defaultdict, namedtuple

## Namedtuples are lighter than classes
Attribute = namedtuple('Attr', ['name','value'])

class Key_Value:
    def __init__(self, key=None, value=None, key_type=None, components=None):
        self.key = key
        self.value = value
        self.key_type = key_type
        self.components = components
    
    def __repr__(self):
        return f"K/V(key={self.key}, value={self.value}, key_type={self.key_type}, components={self.components})"

class Entity_Object:
    def __init__(self, name=None, object_id=None):
        self.name = name
        self.object_id = object_id
        self.attributes: list[Attribute] = []
        self.aggregates: defaultdict[str, list[Attribute]] = defaultdict(list) ## 1:1
        self.aggregate_arrays: defaultdict[str, defaultdict[str, list[Attribute]]] = defaultdict(lambda: defaultdict(list)) ## 1:N | N:N
    
    def add_entity_attribute(self, attribute_name, value): ## entity.attrib -> value
        self.attributes.append(Attribute(attribute_name, value))
    
    def add_aggregate_attribute(self, aggregate_name, attribute_name, value): ## entity.aggregate.attrib -> value
        self.aggregates[aggregate_name].append(Attribute(attribute_name, value))
    
    def add_aggregate_array_attribute(self, array_name, index, attribute_name, value): ## entity.aggregate[index].attrib -> value
        self.aggregate_arrays[array_name][index].append(Attribute(attribute_name, value))
    
    def __repr__(self): ## To make EO output (print) readable
        string = f"E/O(entity_name={self.name}, object_id={self.object_id}, attributes={dict(self.attributes)}, "
        string += f"aggregates={dict({k: dict(v) for k,v in self.aggregates.items()})}, "
        string += f"aggregate_arrays={dict({k: dict(v) for k,v in dict({k: dict(v) for k,v in self.aggregates.items()}).items()})})"
        return string

class Column:
    def __init__(self, name=None):
        self.name = name
        self.data_type = None
        self.nullable = False
        self.values = []
    
    def __repr__(self):
        return f"(data_type={self.data_type}, nullable={self.nullable})"

class Table:
    def __init__(self, name=None):
        self.name = name
        self.columns: defaultdict[str, Column] = defaultdict(lambda: Column(name=None))
        self.primary_key = None
        self.foreign_keys = set()
        self.count = 0 ## To determine nullability
    
    def add_column(self, column_name, value):
        self.columns[column_name].name = column_name
        self.columns[column_name].values.append(str(value))
    
    def set_primary_key(self, column_name):
        self.primary_key = column_name
    
    def add_foreign_key(self, column_name, referenced_table, referenced_column):
        self.foreign_keys.add((column_name, referenced_table, referenced_column))
        
    def __repr__(self):
        return f"Table(name={self.name}, count={self.count}, columns={dict({k: v for k,v in self.columns.items()})}, primary_key={self.primary_key}, foreign_keys={self.foreign_keys})"