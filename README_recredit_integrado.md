# Recredit - app integrado

Este app reúne 3 funções em uma única interface Streamlit:

1. Converter PDF/Excel em TXT de importação
2. Aplicar regra de unidade em um TXT existente
3. Gerar planilha de cobrança por WhatsApp a partir de PDF de inadimplência

## Instalação

```bash
py -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements_recredit_integrado.txt
```

## Execução

```bash
streamlit run app_streamlit_recredit_integrado.py
```

## Arquivos usados

- `app_streamlit_recredit_integrado.py`
- `importador_core_patched.py`
- `requirements_recredit_integrado.txt`

## Observações

- A leitura de PDF tenta usar `pypdf` primeiro e, se falhar, usa `pdfplumber`.
- A aba de cobrança usa o mesmo princípio para evitar travar o app logo na inicialização.
- Para a regra `orlando`, informe o código alvo no campo específico.
