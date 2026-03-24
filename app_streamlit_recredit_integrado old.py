from __future__ import annotations

import io
import re
import urllib.parse
from typing import List

import pandas as pd
import streamlit as st

from importador_core_patched import (
    DEFAULT_CONFIG,
    ImportadorRecredit,
    aplicar_regra_unidade_txt,
    carregar_config_json,
)

st.set_page_config(page_title="Recredit - Importador + Cobrança", page_icon="🏢", layout="wide")


# =========================
# Helpers importador
# =========================
def boletos_para_df(boletos):
    rows = []
    for b in boletos:
        rows.append(
            {
                "unidade": b.unidade,
                "vencimento": b.vencimento,
                "qtde_rateios": len(b.rateios),
                "historico": b.historico,
                "valor_total": round(b.total, 2),
                "rateios": " | ".join(
                    f"{r.codigo} - {r.historico}: {r.valor:.2f}".replace(".", ",")
                    for r in b.rateios
                ),
            }
        )
    return pd.DataFrame(rows)


def txt_para_download_bytes(texto: str, encoding: str = "cp1252") -> bytes:
    try:
        return texto.encode(encoding)
    except Exception:
        return texto.encode("utf-8")


# =========================
# Helpers cobrança whatsapp
# =========================
def get_pdf_reader():
    erros = []
    try:
        from pypdf import PdfReader
        return PdfReader
    except Exception as e:
        erros.append(f"pypdf: {e}")
    try:
        import pdfplumber

        class _PdfPlumberReader:
            def __init__(self, fileobj):
                self._pdf = pdfplumber.open(fileobj)
                self.pages = self._pdf.pages

        return _PdfPlumberReader
    except Exception as e:
        erros.append(f"pdfplumber: {e}")
    raise RuntimeError("Não foi possível carregar uma biblioteca de leitura de PDF. " + " | ".join(erros))


def br_to_float(value: str) -> float:
    return float(value.replace(".", "").replace(",", "."))


def fmt_currency(value: float | None) -> str:
    if value is None:
        return ""
    s = f"{value:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def clean_name(name: str) -> str:
    name = re.sub(r"\*.*?\*", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def first_name(full_name: str) -> str:
    cleaned = clean_name(full_name)
    parts = cleaned.split()
    return parts[0].title() if parts else ""


def limpar_texto(texto: str) -> str:
    texto = texto.replace("\xa0", " ")
    texto = texto.replace("\r", "\n")
    texto = re.sub(r"[ \t]+", " ", texto)
    return texto


def extrair_condominio(texto: str) -> str:
    padrao = re.compile(r"Condom[ií]nio:\s*\(\d+\)\s*-\s*(.+)", flags=re.IGNORECASE)
    for linha in texto.splitlines():
        linha = linha.strip()
        m = padrao.search(linha)
        if m:
            return m.group(1).strip()
    return "Seu condomínio"


def parse_condomino_line(line: str) -> tuple[str, str, str]:
    content = line.replace("Condômino Atual:", "").strip()
    phone = ""
    document = ""

    phone_match = re.search(r"\s*-\s*Fone:(.*)$", content)
    if phone_match:
        phone = phone_match.group(1).strip()
        content = content[: phone_match.start()].rstrip()

    doc_match = re.search(r"\s*-\s*(CPF|CNPJ):\s*([0-9./-]+)\s*$", content)
    if doc_match:
        document = doc_match.group(2).strip()
        content = content[: doc_match.start()].rstrip()

    name = content.strip()
    return name, document, phone


def extract_phone_candidates(phone_raw: str) -> List[str]:
    if not phone_raw:
        return []

    chunks = re.split(r"[;/]| e ", phone_raw, flags=re.IGNORECASE)
    phones: List[str] = []

    for chunk in chunks:
        digits = re.sub(r"\D", "", chunk)

        if len(digits) >= 13 and digits.startswith("55"):
            digits = digits[2:]

        if len(digits) > 11:
            tail_match = re.search(r"(\d{2}9?\d{8})$", digits)
            if tail_match:
                digits = tail_match.group(1)

        if len(digits) in (10, 11):
            phones.append(digits)

    deduped: List[str] = []
    for phone in phones:
        if phone not in deduped:
            deduped.append(phone)

    return deduped


def parse_total_value(total_line: str) -> float | None:
    if not total_line:
        return None

    match = re.search(r"(-?[\d.]+,\d{2})\d*$", total_line)
    if match:
        return br_to_float(match.group(1))
    return None


def classify_charge(entries: list[dict]) -> str:
    statuses = {entry["status"] for entry in entries if entry["status"]}

    if "E" in statuses:
        return "execucao"
    if "J" in statuses:
        return "juridica"
    if "S" in statuses or "A" in statuses:
        return "acordo"
    return "amistosa"


def build_message(
    kind: str,
    first: str,
    unit: str,
    condominio: str,
    due_dates: list[str],
    total_value: float | None,
    msg_amistosa: str,
    msg_acordo: str,
    msg_juridica: str,
    msg_execucao: str,
) -> str:
    due_text = ", ".join(due_dates)
    total_text = fmt_currency(total_value)

    placeholders = {
        "{primeiro_nome}": first,
        "{unidade}": unit,
        "{condominio}": condominio,
        "{vencimentos}": due_text,
        "{valor_total}": total_text,
    }

    template_map = {
        "amistosa": msg_amistosa,
        "acordo": msg_acordo,
        "juridica": msg_juridica,
        "execucao": msg_execucao,
    }
    message = template_map.get(kind, msg_amistosa)

    for chave, valor in placeholders.items():
        message = message.replace(chave, valor)
    return message


def parse_pdf_bytes_cobranca(
    pdf_bytes: bytes,
    msg_amistosa: str,
    msg_acordo: str,
    msg_juridica: str,
    msg_execucao: str,
) -> pd.DataFrame:
    PdfReader = get_pdf_reader()
    reader = PdfReader(io.BytesIO(pdf_bytes))
    full_text = []
    raw_lines = []

    for page in reader.pages:
        page_text = page.extract_text() or ""
        full_text.append(page_text)
        raw_lines.extend([line.strip() for line in page_text.splitlines() if line.strip()])

    texto = limpar_texto("\n".join(full_text))
    condominio = extrair_condominio(texto)

    ignored_prefixes = (
        "Unidade",
        "Sistema de Condomínios",
        "Atualização de Boletos",
        "Página ",
        "Histórico do Lançamento",
        "Condomínio:",
        "Da Unidade",
        "Da emissão",
        "até ",
        "KGR RECEBIMENTOS",
        "Total:",
        "J. Em cobrança judicial",
    )

    lines = [
        line
        for line in raw_lines
        if not any(line.startswith(prefix) for prefix in ignored_prefixes)
        and "<PARSED TEXT FOR PAGE:" not in line
    ]

    entry_re = re.compile(
        r"^(?:(?P<status>[A-Z])\s+)?(?P<unit>[A-Z]-\d{3})\s+"
        r"(?P<desc>.+?)\s+(?P<due>\d{2}/\d{2}/\d{4})\s+[-\d., ]+$"
    )

    rows = []
    current_entries = []

    for i, line in enumerate(lines):
        entry_match = entry_re.match(line)
        if entry_match:
            current_entries.append(
                {
                    "status": entry_match.group("status") or "",
                    "unit": entry_match.group("unit"),
                    "desc": entry_match.group("desc").strip(),
                    "due": entry_match.group("due"),
                }
            )
            continue

        if line == "* Total Unidade *":
            if not current_entries:
                continue

            cond_line = lines[i + 1] if i + 1 < len(lines) else ""
            address_line = lines[i + 2] if i + 2 < len(lines) else ""
            total_line = lines[i + 3] if i + 3 < len(lines) else ""

            unit = current_entries[-1]["unit"]
            name_raw, document, phone_raw = parse_condomino_line(cond_line)
            name = clean_name(name_raw)
            f_name = first_name(name_raw)
            phones = extract_phone_candidates(phone_raw)
            phone_main = phones[0] if phones else ""
            charge_type = classify_charge(current_entries)
            total_value = parse_total_value(total_line)
            due_dates = [entry["due"] for entry in current_entries]
            message = build_message(
                charge_type,
                f_name,
                unit,
                condominio,
                due_dates,
                total_value,
                msg_amistosa,
                msg_acordo,
                msg_juridica,
                msg_execucao,
            )

            if charge_type == "execucao" or not phone_main:
                whatsapp_link = ""
            else:
                whatsapp_link = f"https://wa.me/55{phone_main}?text={urllib.parse.quote(message)}"

            rows.append(
                {
                    "condominio": condominio,
                    "unidade": unit,
                    "nome": name,
                    "primeiro_nome": f_name,
                    "documento": document,
                    "telefone_bruto": phone_raw,
                    "telefone_cadastro": phone_main,
                    "telefones_encontrados": " | ".join(phones),
                    "tipo_cobranca": charge_type,
                    "qtde_titulos": len(current_entries),
                    "vencimentos_aberto": " | ".join(due_dates),
                    "resumo_lancamentos": " | ".join(entry["desc"] for entry in current_entries),
                    "valor_total_atualizado": total_value,
                    "endereco": address_line.replace("Endereço:", "").strip(),
                    "link_whatsapp": whatsapp_link,
                    "mensagem_whatsapp": message,
                }
            )

            current_entries = []

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    order = {"execucao": 0, "juridica": 1, "acordo": 2, "amistosa": 3}
    df["_ordem_tipo"] = df["tipo_cobranca"].map(order).fillna(99)
    df = df.sort_values(["_ordem_tipo", "unidade"]).drop(columns=["_ordem_tipo"])
    return df


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
        ws = writer.sheets["dados"]
        for idx, col in enumerate(df.columns, start=1):
            max_len = max(len(str(col)), *(len(str(v)) for v in df[col].fillna(""))) if len(df) else len(str(col))
            ws.column_dimensions[chr(64 + min(idx, 26))].width = min(max(max_len + 2, 12), 90)
    return output.getvalue()


# =========================
# Interface
# =========================
st.title("🏢 Recredit - Importador e Cobrança")

aba1, aba2, aba3 = st.tabs([
    "Converter PDF/Excel em TXT",
    "Aplicar regra em TXT",
    "Cobrança WhatsApp",
])

with aba1:
    st.subheader("Converter espelho em TXT de importação")
    col1, col2 = st.columns([1, 1])

    with col1:
        arquivo = st.file_uploader(
            "Arquivo de entrada",
            type=["pdf", "xlsx", "xlsm", "xls"],
            key="arquivo_importador",
        )
        config_json = st.file_uploader(
            "Configuração JSON opcional",
            type=["json"],
            key="config_importador",
        )
        regra_unidade = st.selectbox(
            "Regra de unidade",
            options=["padrao", "portal", "mafalda", "orlando"],
            index=0,
            key="regra_importador",
        )
        codigo_orlando = st.text_input("Código alvo Orlando", value="101", key="orlando_importador")

    with col2:
        st.caption("Cabeçalho do TXT")
        emp_codigo = st.text_input("EMP_CODIGO", value=DEFAULT_CONFIG["emp_codigo"])
        emp_nomemp = st.text_input("EMP_NOMEMP", value=DEFAULT_CONFIG["emp_nomemp"])
        emp_cegece = st.text_input("EMP_CEGECE", value=DEFAULT_CONFIG["emp_cegece"])
        administradora_nome = st.text_input("Administradora nome", value=DEFAULT_CONFIG["administradora_nome"])
        administradora_fantasia = st.text_input("Administradora fantasia", value=DEFAULT_CONFIG["administradora_fantasia"])
        blq_datemi = st.text_input("BLQ_DATEMI", value=DEFAULT_CONFIG["blq_datemi"])

    if arquivo is not None:
        try:
            config_extra = carregar_config_json(config_json.read()) if config_json else {}
            config = {
                **config_extra,
                "emp_codigo": emp_codigo,
                "emp_nomemp": emp_nomemp,
                "emp_cegece": emp_cegece,
                "administradora_nome": administradora_nome,
                "administradora_fantasia": administradora_fantasia,
                "blq_datemi": blq_datemi,
            }
            core = ImportadorRecredit(config=config)
            boletos = core.parse_file(arquivo.read(), arquivo.name)

            if not boletos:
                st.warning("Nenhum boleto foi reconhecido nesse arquivo.")
            else:
                df_prev = boletos_para_df(boletos)
                st.success(f"{len(df_prev)} boleto(s) reconhecido(s).")
                st.dataframe(df_prev, use_container_width=True, hide_index=True)

                txt = core.gerar_txt(boletos, regra_unidade="padrao")
                txt = aplicar_regra_unidade_txt(txt, regra_unidade, codigo_orlando)

                avisos = []
                vazios = df_prev[df_prev["unidade"].astype(str).str.strip() == ""]
                if len(vazios):
                    avisos.append(f"{len(vazios)} unidade(s) vazias.")
                if (df_prev["valor_total"] <= 0).any():
                    avisos.append("Há boleto(s) com valor total zerado ou negativo.")
                if avisos:
                    st.warning(" | ".join(avisos))

                st.download_button(
                    "Baixar TXT de importação",
                    data=txt_para_download_bytes(txt, config.get("encoding_saida", "cp1252")),
                    file_name=f"{arquivo.name.rsplit('.', 1)[0]}.txt",
                    mime="text/plain",
                    use_container_width=True,
                )
                with st.expander("Prévia do TXT"):
                    st.code(txt[:12000])
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")

with aba2:
    st.subheader("Aplicar regra em um TXT já existente")
    txt_file = st.file_uploader("Selecione o TXT", type=["txt"], key="txt_regra")
    regra_txt = st.selectbox(
        "Regra de unidade",
        options=["padrao", "portal", "mafalda", "orlando"],
        index=0,
        key="regra_txt",
    )
    codigo_orlando_txt = st.text_input("Código alvo Orlando", value="101", key="orlando_txt")

    if txt_file is not None:
        try:
            bruto = txt_file.read()
            try:
                texto = bruto.decode("cp1252")
            except Exception:
                texto = bruto.decode("utf-8", errors="replace")

            convertido = aplicar_regra_unidade_txt(texto, regra_txt, codigo_orlando_txt)
            st.download_button(
                "Baixar TXT ajustado",
                data=txt_para_download_bytes(convertido, "cp1252"),
                file_name=f"{txt_file.name.rsplit('.', 1)[0]}_ajustado.txt",
                mime="text/plain",
                use_container_width=True,
            )
            c1, c2 = st.columns(2)
            with c1:
                st.caption("TXT original")
                st.code(texto[:10000])
            with c2:
                st.caption("TXT ajustado")
                st.code(convertido[:10000])
        except Exception as e:
            st.error(f"Erro ao aplicar regra: {e}")

with aba3:
    st.subheader("Gerador de cobrança por WhatsApp")
    with st.sidebar:
        st.header("Modelos de mensagem")
        st.caption("Placeholders: {primeiro_nome}, {unidade}, {condominio}, {vencimentos}, {valor_total}")
        msg_amistosa = st.text_area(
            "Mensagem amistosa",
            value=(
                "Olá, {primeiro_nome}. Tudo bem?\n\n"
                "Identificamos pendência(s) referente(s) à unidade {unidade} do condomínio {condominio}, "
                "com vencimento(s) em {vencimentos}, até a presente data.\n\n"
                "Caso já tenha realizado o pagamento, por favor desconsidere esta mensagem.\n"
                "Se preferir, podemos te auxiliar com segunda via do boleto ou esclarecimentos.\n\n"
                "Ficamos à disposição para ajudar."
            ),
            height=220,
        )
        msg_acordo = st.text_area(
            "Mensagem de acordo",
            value=(
                "Olá, {primeiro_nome}. Tudo bem?\n\n"
                "Constam pendência(s) da unidade {unidade} do condomínio {condominio}, "
                "com vencimento(s) em {vencimentos}, até a presente data.\n\n"
                "Peço, por gentileza, que nos retorne para alinharmos a regularização."
            ),
            height=180,
        )
        msg_juridica = st.text_area(
            "Mensagem jurídica",
            value=(
                "Olá, {primeiro_nome}. Tudo bem?\n\n"
                "Constam pendência(s) da unidade {unidade} do condomínio {condominio}, com vencimento(s) em {vencimentos}, "
                "até a presente data.\n\n"
                "Esta unidade possui apontamento em cobrança jurídica. Para tratarmos corretamente, peço que nos retorne neste contato."
            ),
            height=200,
        )
        msg_execucao = st.text_area(
            "Mensagem interna para execução",
            value=(
                "Olá, {primeiro_nome}. Tudo bem?\n\n"
                "Constam pendência(s) da unidade {unidade} do condomínio {condominio}, com vencimento(s) em {vencimentos}, "
                "totalizando R$ {valor_total} até a presente data.\n\n"
                "Esta unidade está em fase de execução. O caso deve seguir tratamento interno."
            ),
            height=200,
        )

    pdf_cobranca = st.file_uploader("Selecione o PDF de inadimplência", type=["pdf"], key="pdf_cobranca")

    if pdf_cobranca is not None:
        try:
            df = parse_pdf_bytes_cobranca(
                pdf_cobranca.read(),
                msg_amistosa,
                msg_acordo,
                msg_juridica,
                msg_execucao,
            )
            if df.empty:
                st.warning("Nenhuma unidade foi encontrada nesse PDF.")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Unidades processadas", len(df))
                c2.metric("Amistosas", int((df["tipo_cobranca"] == "amistosa").sum()))
                c3.metric("Acordo/Jurídica", int(((df["tipo_cobranca"] == "acordo") | (df["tipo_cobranca"] == "juridica")).sum()))
                c4.metric("Execução", int((df["tipo_cobranca"] == "execucao").sum()))

                filtro_tipo = st.multiselect(
                    "Filtrar por tipo de cobrança",
                    options=sorted(df["tipo_cobranca"].dropna().unique().tolist()),
                    default=sorted(df["tipo_cobranca"].dropna().unique().tolist()),
                )
                termo = st.text_input("Buscar por unidade, nome ou telefone", key="busca_cobranca")
                df_view = df.copy()
                if filtro_tipo:
                    df_view = df_view[df_view["tipo_cobranca"].isin(filtro_tipo)]
                if termo:
                    mask = (
                        df_view["unidade"].fillna("").str.contains(termo, case=False, na=False)
                        | df_view["nome"].fillna("").str.contains(termo, case=False, na=False)
                        | df_view["telefone_cadastro"].fillna("").str.contains(termo, case=False, na=False)
                    )
                    df_view = df_view[mask]

                st.dataframe(df_view, use_container_width=True, hide_index=True)
                excel_bytes = df_to_excel_bytes(df)
                csv_bytes = df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")
                d1, d2 = st.columns(2)
                d1.download_button(
                    "Baixar Excel",
                    data=excel_bytes,
                    file_name="cobranca_whatsapp.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                d2.download_button(
                    "Baixar CSV",
                    data=csv_bytes,
                    file_name="cobranca_whatsapp.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception as e:
            st.error(f"Erro ao processar o PDF: {e}")
    else:
        st.info("Envie um PDF para começar.")
