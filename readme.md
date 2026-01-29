# Conversor CSV / Excel → JSON & JSONL 🐍

![Python](https://www.python.org/static/community_logos/python-logo-master-v3-TM.png)

## Visão geral
Este projeto é um **conversor robusto de dados tabulares** (CSV e Excel) para **JSON** e **JSONL**, projetado para lidar com **arquivos muito grandes**, mantendo baixo consumo de memória, alta confiabilidade e excelente experiência para desenvolvedores.

Ele foi pensado para **ambientes reais de produção**, onde arquivos podem ter milhões de linhas, campos gigantes e problemas de encoding.

---

## Objetivo do projeto
Resolver, de forma definitiva, os principais problemas ao converter tabelas grandes para JSON:

- ❌ Estouro de memória  
- ❌ Falhas por campos grandes (`field larger than field limit`)  
- ❌ Falta de visibilidade durante o processamento  
- ❌ Dificuldade de validar estrutura antes do arquivo final  

Este conversor entrega:

- **Streaming real** (linha a linha)
- **Observabilidade** (barra de progresso + `progress.json`)
- **Pré-visualização inteligente** (exemplos reais + templates)
- **Split automático da saída**
- **Compatibilidade com pipelines e APIs**

---

## Linguagem e stack
- **Python 3.10+** (recomendado)
- Bibliotecas:
  - `tqdm` → barra de progresso no terminal *(opcional, mas recomendado)*
  - `openpyxl` → necessário apenas para arquivos `.xlsx`

> Para **CSV**, o script usa majoritariamente a biblioteca padrão do Python.

---

## O que o script faz

### Conversão
- CSV (`.csv`, `.tsv`, `.txt`)
- Excel (`.xlsx`)
- Saída em:
  - **JSON (array)**
  - **JSONL** (1 objeto por linha — ideal para arquivos grandes)

### Robustez
- Suporte a **campos gigantes**
- Detecção automática de **encoding** e **delimitador**
- Remoção de **caracteres invisíveis** (BOM, zero-width, NBSP etc.)
- Proteção contra cabeçalhos duplicados

### Observabilidade
- Barra de progresso no terminal (tqdm)
- Arquivo `progress.json` atualizado durante a execução  
  Ideal para UI, painel ou polling backend.

### Split da saída
- Dividir o arquivo final em:
  - **N partes aproximadamente iguais**
  - **X registros por arquivo**

### Developer Experience (DX)
Antes de gerar o arquivo completo, o script **sempre cria exemplos**:

1. **Exemplos reais**  
   → primeiros N registros do dataset  
2. **Templates**  
   → apenas as chaves, com valores vazios ou `null`  
3. **Arquivo de chaves**  
   → lista das colunas detectadas  

Isso permite que programadores:
- validem o schema
- criem DTOs / interfaces
- integrem APIs
- sem esperar o processamento completo

---

## Estrutura gerada (exemplo)

Ao converter `dados.csv`, são gerados:

```text
dados.examples.real.10.jsonl
dados.examples.template.10.jsonl
dados.examples.keys.json
dados.jsonl               ← arquivo final
progress.json
