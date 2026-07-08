-- Criação das Tabelas de Dimensões (Gold)

CREATE TABLE IF NOT EXISTS dim_produto (
    produto_id INT PRIMARY KEY,
    produto VARCHAR(255) NOT NULL,
    categoria VARCHAR(255) NOT NULL,
    marca VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_local (
    local_id INT PRIMARY KEY,
    cidade VARCHAR(255) NOT NULL,
    estado CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_fornecedor (
    fornecedor_id INT PRIMARY KEY,
    fornecedor VARCHAR(255) NOT NULL
);

-- Criação das Tabelas de Fatos (Gold)

CREATE TABLE IF NOT EXISTS fato_vendas (
    pedido_id INT PRIMARY KEY,
    produto_id INT NOT NULL,
    cliente_id INT NOT NULL,
    local_id INT NOT NULL,
    data_pedido DATE NOT NULL,
    quantidade INT NOT NULL,
    preco_unitario NUMERIC(10, 2) NOT NULL,
    desconto NUMERIC(10, 2) NOT NULL,
    frete NUMERIC(10, 2) NOT NULL,
    valor_total NUMERIC(10, 2) NOT NULL,
    valor_total_sem_desconto NUMERIC(10, 2) NOT NULL,
    canal_venda VARCHAR(100) NOT NULL,
    forma_pagamento VARCHAR(100) NOT NULL,
    status_pedido VARCHAR(100) NOT NULL,
    CONSTRAINT fk_fato_vendas_produto FOREIGN KEY (produto_id) REFERENCES dim_produto(produto_id),
    CONSTRAINT fk_fato_vendas_cliente FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT fk_fato_vendas_local FOREIGN KEY (local_id) REFERENCES dim_local(local_id)
);

CREATE TABLE IF NOT EXISTS fato_devolucoes (
    devolucao_id INT PRIMARY KEY,
    pedido_id INT NOT NULL,
    produto_id INT NOT NULL,
    cliente_id INT NOT NULL,
    data_devolucao DATE NOT NULL,
    motivo_devolucao VARCHAR(255) NOT NULL,
    status_devolucao VARCHAR(100) NOT NULL,
    valor_devolvido NUMERIC(10, 2) NOT NULL,
    CONSTRAINT fk_fato_devolucoes_produto FOREIGN KEY (produto_id) REFERENCES dim_produto(produto_id),
    CONSTRAINT fk_fato_devolucoes_cliente FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT fk_fato_devolucoes_vendas FOREIGN KEY (pedido_id) REFERENCES fato_vendas(pedido_id)
);

CREATE TABLE IF NOT EXISTS fato_estoque (
    produto_id INT NOT NULL PRIMARY KEY,
    fornecedor_id INT NOT NULL,
    estoque_inicial INT NOT NULL,
    entradas_periodo INT NOT NULL,
    estoque_atual INT NOT NULL,
    estoque_minimo INT NOT NULL,
    custo_unitario NUMERIC(10, 2) NOT NULL,
    centro_distribuicao VARCHAR(255) NOT NULL,
    CONSTRAINT fk_fato_estoque_produto FOREIGN KEY (produto_id) REFERENCES dim_produto(produto_id),
    CONSTRAINT fk_fato_estoque_fornecedor FOREIGN KEY (fornecedor_id) REFERENCES dim_fornecedor(fornecedor_id)
);
