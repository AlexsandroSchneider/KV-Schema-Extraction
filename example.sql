CREATE TABLE Cliente (
    Cliente_id INTEGER PRIMARY KEY,
    nome TEXT,
    email TEXT
);

CREATE TABLE Endereco (
    Endereco_id INTEGER PRIMARY KEY,
    rua TEXT,
    cidade TEXT,
    estado TEXT
);

-- Restrição UNIQUE em Endereco_id torna o relacionamento (1:1)
-- Endereco é agregado à Funcionario
CREATE TABLE Funcionario (
    Funcionario_id INTEGER PRIMARY KEY,
    nome TEXT,
    cargo TEXT,
    Endereco_id INTEGER UNIQUE,
    FOREIGN KEY (Endereco_id) REFERENCES Endereco(Endereco_id)
);

CREATE TABLE Filme (
    Filme_id INTEGER PRIMARY KEY,
    titulo TEXT,
    ano_lancamento INTEGER,
    classificacao TEXT
);

CREATE TABLE Categoria (
    Categoria_id INTEGER PRIMARY KEY,
    nome TEXT
);

-- Tabela de junção (N:N) entre Filme e Categoria
-- Filme ou Categoria recebe os atributos dessa tabela
CREATE TABLE Filme_Categorias (
    Filme_id INTEGER,
    Categoria_id INTEGER,
    PRIMARY KEY (Filme_id, Categoria_id),
    FOREIGN KEY (Filme_id) REFERENCES Filme(Filme_id),
    FOREIGN KEY (Categoria_id) REFERENCES Categoria(Categoria_id)
);

CREATE TABLE Pagamento (
    Pagamento_id INTEGER PRIMARY KEY,
    valor DECIMAL,
    data_pagamento DATETIME,
);

-- Restrição UNIQUE em Pagamento_id. Pagamento é agregado à Aluguel
CREATE TABLE Aluguel (
    Aluguel_id INTEGER PRIMARY KEY,
    Cliente_id INTEGER,
    Filme_id INTEGER,
    Funcionario_id INTEGER,
    data_aluguel DATE,
    data_devolucao DATE,
    Pagamento_id INTEGER UNIQUE,
    FOREIGN KEY (Cliente_id) REFERENCES Cliente(Cliente_id),
    FOREIGN KEY (Filme_id) REFERENCES Filme(Filme_id),
    FOREIGN KEY (Funcionario_id) REFERENCES Funcionario(Funcionario_id)
    FOREIGN KEY (Pagamento_id) REFERENCES Pagamento(Pagamento_id)
);