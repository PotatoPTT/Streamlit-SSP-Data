CREATE TABLE regioes (
    id smallserial PRIMARY KEY,
    nome VARCHAR(60) NOT NULL UNIQUE
);

CREATE TABLE municipios (
    id smallserial PRIMARY KEY,
    nome VARCHAR(60) NOT NULL,
    latitude REAL,
    longitude REAL,
    regiao_id smallint NOT NULL REFERENCES regioes(id)
);

CREATE TABLE crimes (
    id smallserial PRIMARY KEY,
    natureza VARCHAR(60) NOT NULL UNIQUE
);

CREATE TABLE ocorrencias (
    ano smallint NOT NULL,
    mes smallint NOT NULL CHECK (mes >= 1 AND mes <= 12),
    municipio_id smallint NOT NULL REFERENCES municipios(id),
    crime_id smallint NOT NULL REFeERENCES crimes(id),
    quantidade smallint NOT NULL CHECK (quantidade >= 0),
    PRIMARY KEY (ano, mes, municipio_id, crime_id)
);

CREATE TABLE solicitacoes_modelo (
    id BIGSERIAL PRIMARY KEY,
    parametros JSONB NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDENTE', 'PROCESSANDO', 'CONCLUIDO', 'FALHOU', 'EXPIRADO')),
    data_solicitacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_atualizacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mensagem_erro TEXT,
    arquivo BYTEA,
    UNIQUE (parametros)
);